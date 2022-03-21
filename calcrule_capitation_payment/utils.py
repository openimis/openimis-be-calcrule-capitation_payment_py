import logging
from django.db import connection
from django.db.models import Value, Q, Sum, Count, F, Prefetch
from django.db.models.functions import Coalesce
from django.contrib.contenttypes.models import ContentType

from claim.models import ClaimItem, Claim, ClaimService
from insuree.models import InsureePolicy
from invoice.models import Bill
from policy.models import Policy
from location.models import Location, HealthFacility
from claim_batch.models import CapitationPayment

logger = logging.getLogger(__name__)


#@deprecated
def capitation_report_data_for_submit(audit_user_id, location_id, period, year):
    # moved from claim_batch module
    capitation_payment_products = []
    for svc_item in [ClaimItem, ClaimService]:
        capitation_payment_products.extend(
            svc_item.objects
                    .filter(claim__status=Claim.STATUS_VALUATED)
                    .filter(claim__validity_to__isnull=True)
                    .filter(validity_to__isnull=True)
                    .filter(status=svc_item.STATUS_PASSED)
                    .annotate(prod_location=Coalesce("product__location_id", Value(-1)))
                    .filter(prod_location=location_id if location_id else -1)
                    .values('product_id')
                    .distinct()
        )

    region_id, district_id, region_code, district_code = get_capitation_region_and_district(location_id)
    for product in set(map(lambda x: x['product_id'], capitation_payment_products)):
        params = {
            'region_id': region_id,
            'district_id': district_id,
            'prod_id': product,
            'year': year,
            'month': period,
        }
        is_report_data_available = get_commision_payment_report_data(params)
        if not is_report_data_available:
            process_capitation_payment_data(params)
        else:
            logger.debug(F"Capitation payment data for {params} already exists")


#@deprecated
def get_capitation_region_and_district(location_id):
    if not location_id:
        return None, None
    location = Location.objects.get(id=location_id)

    region_id = None
    region_code = None
    district_id = None
    district_code = None

    if location.type == 'D':
        district_id = location_id
        district_code = location.code
        region_id = location.parent.id
        region_code = location.parent.code
    elif location.type == 'R':
        region_id = location.id
        region_code = location.code

    return region_id, district_id, region_code, district_code


#@deprecated
def process_capitation_payment_data(params):
    with connection.cursor() as cur:
        # HFLevel based on
        # https://github.com/openimis/web_app_vb/blob/2492c20d8959e39775a2dd4013d2fda8feffd01c/IMIS_BL/HealthFacilityBL.vb#L77
        _execute_capitation_payment_procedure(cur, 'uspCreateCapitationPaymentReportData', params)


#@deprecated
def get_commision_payment_report_data(params):
    with connection.cursor() as cur:
        # HFLevel based on
        # https://github.com/openimis/web_app_vb/blob/2492c20d8959e39775a2dd4013d2fda8feffd01c/IMIS_BL/HealthFacilityBL.vb#L77
        _execute_capitation_payment_procedure(cur, 'uspSSRSRetrieveCapitationPaymentReportData', params)

        # stored proc outputs several results,
        # we are only interested in the last one
        next = True
        data = None
        while next:
            try:
                data = cur.fetchall()
            except Exception as e:
                pass
            finally:
                next = cur.nextset()
    return data


#@deprecated
def _execute_capitation_payment_procedure(cursor, procedure, params):
    sql = F"""\
                DECLARE @HF AS xAttributeV;

                INSERT INTO @HF (Code, Name) VALUES ('D', 'Dispensary');
                INSERT INTO @HF (Code, Name) VALUES ('C', 'Health Centre');
                INSERT INTO @HF (Code, Name) VALUES ('H', 'Hospital');

                EXEC [dbo].[{procedure}]
                    @RegionId = %s,
                    @DistrictId = %s,
                    @ProdId = %s,
                    @Year = %s,
                    @Month = %s,	
                    @HFLevel = @HF
            """

    cursor.execute(sql, (
        params.get('region_id', None),
        params.get('district_id', None),
        params.get('prod_id', 0),
        params.get('year', 0),
        params.get('month', 0),
    ))


def check_bill_not_exist(instance, health_facility, payment_plan, **kwargs):
    if instance.__class__.__name__ == "BatchRun":
        batch_run = instance
        content_type = ContentType.objects.get_for_model(batch_run.__class__)
        code = f"" \
            f"CP-{payment_plan.code}-{health_facility.code}" \
            f"-{batch_run.run_year}-{batch_run.run_month}"
        bills = Bill.objects.filter(
            subject_type=content_type,
            subject_id=batch_run.id,
            thirdparty_id=health_facility.id,
            code=code
        )
        if bills.exists() == False:
            return True


def generate_capitation(product, start_date, end_date, allocated_contribution):
    population_matter = product.weight_population > 0 or product.weight_nb_families > 0
    year = end_date.year
    month = end_date.month
    if product.weight_insured_population > 0 or product.weight_nb_insured_families > 0 \
            or population_matter:
        # get location (district) linked to the product --> to be 
        sum_pop, sum_families = 1, 1
        if population_matter:
            sum_pop, sum_families = get_product_sum_population(product)
        sum_insurees = 1
        # get the total number of insuree
        if product.weight_insured_population > 0:
            sum_insurees = get_product_sum_insurees(product, start_date, end_date)
        # get the total number of insured family
        sum_insured_families = 1
        if product.weight_nb_insured_families > 0:
            sum_insured_families = get_product_sum_policies(product, start_date, end_date)
        # get the claim data
        sum_claim_adjusted_amount, sum_visits = 1, 1
        if product.weight_nb_visits > 0 or product.weight_adjusted_amount > 0:
            sum_claim_adjusted_amount, sum_visits = get_product_sum_claim(product, start_date, end_date)

        # select HF concerned with capitation within the product location (new HF will come from claims)
        health_facilities = get_product_hf_filter(product, get_capitation_health_facilites(product, start_date, end_date))
        health_facilities = health_facilities\
            .prefetch_related(Prefetch('location', queryset=Location.objects.filter(validity_to__isnull=True)))\
            .prefetch_related(Prefetch('location__parent', queryset=Location.objects.filter(validity_to__isnull=True)))

        # create n capitaiton report for each facilities
        for health_facility in health_facilities:
            # we might need to create the capitation report here with all the
            # common fields and run a class method generate_capitation_health_facility(product, hf)
            generate_capitation_health_facility(
                product, health_facility, allocated_contribution,
                sum_insurees, sum_insured_families, sum_pop,
                sum_families, sum_claim_adjusted_amount, sum_visits,
                year, month, start_date, end_date
            )


def get_product_hf_filter(product, queryset):
    # takes all HF if not level config is defined (ie. no filter added)
    if product.capitation_sublevel_1 is not None or product.capitation_sublevel_2 is not None \
            or product.capitation_sublevel_3 is not None or product.capitation_sublevel_4 is not None:
        # take the HF that match level and sublevel OR level if sublevel is not set in product
        queryset = queryset\
            .filter(
                (Q(level=product.capitation_level_1) &\
                    (Q(sub_level=product.capitation_sublevel_1) | Q(sub_level__isnull=True))) |\
                (Q(level=product.capitation_level_2) &\
                    (Q(sub_level=product.capitation_sublevel_2) | Q(sub_level__isnull=True))) |\

                (Q(level=product.capitation_level_3) &\
                    (Q(sub_level=product.capitation_sublevel_3) | Q(sub_level__isnull=True))) |\

                (Q(level=product.capitation_level_4) &\
                    (Q(sub_level=product.capitation_sublevel_4) | Q(sub_level__isnull=True)))
            )
    return queryset


def generate_capitation_health_facility(
        product, health_facility, allocated_contribution, sum_insurees, sum_insured_families,
        sum_pop, sum_families, sum_adjusted_amount, sum_visits, year, month, start_date, end_date
):
    population_matter = product.weight_population > 0 or product.weight_nb_families > 0

    sum_hf_pop, sum_hf_families = 0, 0
    # get the sum of pop
    if population_matter:
        sum_hf_pop, sum_hf_families = get_hf_sum_population(health_facility)

    # get the sum of insuree
    sum_hf_insurees = 0
    if product.weight_insured_population > 0:
        sum_hf_insurees = get_product_sum_insurees(product, start_date, end_date, health_facility)

    # get the sum of policy/insureed families
    sum_hf_insured_families = 0
    if product.weight_nb_insured_families > 0:
        sum_hf_insured_families = get_product_sum_policies(product, start_date, end_date, health_facility)

    sum_hf_claim_adjusted_amount, sum_hf_visits = 0, 0
    if product.weight_nb_visits > 0 or product.weight_adjusted_amount > 0:
        sum_hf_claim_adjusted_amount, sum_hf_visits = get_product_sum_claim(product, start_date, end_date, health_facility)

    # ammont available for all HF capitation
    allocated = (allocated_contribution * product.share_contribution) / 100

    # Allocated ammount for the Prodcut (common for all HF)
    alc_contri_population = (allocated * product.weight_population) / 100
    alc_contri_num_families = (allocated * product.weight_nb_families) / 100
    alc_contri_ins_population = (allocated * product.weight_insured_population) / 100
    alc_contri_ins_families = (allocated * product.weight_nb_insured_families) / 100
    alc_contri_visits = (allocated * product.weight_nb_visits) / 100
    alc_contri_adjusted_amount = (allocated * product.weight_adjusted_amount) / 100

    # unit  (common for all HF)
    up_population = alc_contri_population / sum_pop if sum_pop > 0 else 0
    up_num_families = alc_contri_num_families / sum_families if sum_families > 0 else 0
    up_ins_population = alc_contri_ins_population / sum_insurees if sum_insurees > 0 else 0
    up_ins_families = alc_contri_ins_families / sum_insured_families if sum_insured_families > 0 else 0
    up_visits = alc_contri_visits / sum_visits if sum_visits > 0 else 0
    up_adjusted_amount = alc_contri_adjusted_amount / sum_adjusted_amount if sum_adjusted_amount > 0 else 0

    # amount for this HF
    total_population = sum_hf_pop * up_population
    total_families = sum_hf_families * up_num_families
    total_ins_population = sum_hf_insurees * up_ins_population
    total_ins_families = sum_hf_insured_families * up_ins_families
    total_claims = sum_hf_visits * up_visits
    total_adjusted = sum_hf_claim_adjusted_amount * up_adjusted_amount

    # overall total
    payment_cathment = total_population + total_families + total_ins_population + total_ins_families

    # Create the CapitationPayment so it can be retrieved from the invoice to generate the legacy reports
    if payment_cathment > 0:
        capitation = \
            CapitationPayment(
                year=year,
                month=month,
                product=product,
                health_facility=health_facility,
                region_code=health_facility.location.parent.code,
                region_name=health_facility.location.parent.code,
                district_code=health_facility.location.code,
                district_name=health_facility.location.code,
                health_facility_code=health_facility.code,
                health_facility_name=health_facility.name,
                hf_level=health_facility.level,
                hf_sublevel=health_facility.sub_level,
                total_population=total_population,
                total_families=total_families,
                total_insured_insuree=total_ins_population,
                total_insured_families=total_ins_families,
                total_claims=total_claims,
                total_adjusted=total_adjusted,
                alc_contri_population=alc_contri_population,
                alc_contri_num_families=alc_contri_num_families,
                alc_contri_ins_population=alc_contri_ins_population,
                alc_contri_ins_families=alc_contri_ins_families,
                payment_cathment=total_population + total_families + total_ins_population + total_ins_families,
                up_population=up_population,
                up_num_families=up_num_families,
                up_ins_population=up_ins_population,
                up_ins_families=up_ins_families,
                up_visits=up_visits,
                up_adjusted_amount=up_adjusted_amount
            )
        capitation.save()
    # TODO create bill with Capitation in the json_ext_details


# TODO  below might  be move to Product Module
def get_product_districts(product):
    districts = Location.objects.filter(validity_to__isnull=True)
     # if location null, it means all
    if product.location is None:
        districts = districts.all()
    elif product.location.type == 'D':
        # ideally we should just return the object but the caller will expect a queryset not an object
        districts = districts.filter(id=product.location.id)
    elif product.location.type == 'R':
        districts = districts.filter(parent_id=product.location.id)
    else:
        return None
    return districts


def get_product_villages(product):
    districts = get_product_districts(product)
    villages = None
    if districts is not None:
        villages = Location.objects.filter(validity_to__isnull=True)\
                .filter(parent__parent__in=districts)
    return villages


def get_capitation_health_facilites(product, start_date, end_date):
    districts = get_product_districts(product)
    health_facilities_districts = HealthFacility.objects\
        .filter(validity_to__isnull=True)\
        .filter(location__in=districts)
    # might need to add the items/services status
    health_facilities_off_districts = HealthFacility.objects\
        .filter(validity_to__isnull=True)\
        .filter(claim__validity_to__isnull=True)\
        .filter(claim__date_processed__lte=end_date)\
        .filter(claim__date_processed__gt=start_date)\
        .filter((Q(claim__items__product=product) & Q(claim__items__validity_to__isnull=True))
                | (Q(claim__services__product=product) & Q(claim__services__validity_to__isnull=True)))

    if health_facilities_districts is not None:
        health_facilities = get_product_hf_filter(product, health_facilities_districts | health_facilities_off_districts).distinct()
        return health_facilities
    else:
        return None


def get_hf_sum_population(health_facility):
    pop = Location.objects.filter(validity_to__isnull=True)\
            .filter(catchments__health_facility=health_facility)\
            .filter(catchments__validity_to__isnull=True)\
            .annotate(sum_pop=Sum((F('male_population')+F('female_population')+F('other_population'))*F('catchments__catchment')/100))\
            .annotate(sum_families=Sum((F('male_population')+F('female_population')+F('other_population'))*F('catchments__catchment')/100))

    sum_pop, sum_families = 0, 0
    for p in pop:
        sum_pop += p.sum_pop
        sum_families += p.sum_families

    return sum_pop, sum_families


def get_product_sum_insurees(product, start_date, end_date, health_facility=None):
    villages = get_product_villages(product)
    if villages is not None:
        insurees = InsureePolicy.objects\
            .filter(validity_to__isnull=True)\
            .filter(insuree__family__location__in=villages)\
            .filter(policy__expiry_date__gte=start_date)\
            .filter(policy__effective_date__lte=start_date)\
            .filter(policy__product=product)
        # filter based on catchement if HF is defined
        if health_facility is None:
            insurees = insurees.annotate(sum=Count('id')/100)
        else:
            insurees = insurees.filter(policy__family__location__catchments__health_facility=health_facility)\
                .filter(policy__family__location__catchments__validity_to__isnull=True)\
                .annotate(sum=Sum(F('policy__family__location__catchments__catchment'))*Count('id')/100)
        sum_insuree = 0
        for insuree in insurees:
            sum_insuree += insuree.sum
        return sum_insuree
    else:
        return 0


def get_product_sum_policies(product, start_date, end_date, health_facility=None):
    villages = get_product_villages(product)
    if villages is not None:
        policies = Policy.objects\
            .filter(validity_to__isnull=True)\
            .filter(family__location__in=villages)\
            .filter(expiry_date__gte=start_date)\
            .filter(effective_date__lte=start_date)\
            .filter(product=product)
        # filter based on catchement if HF is defined
        if health_facility is None:
                policies = policies.annotate(sum=Count('id')/100)
        else:
            policies = policies.filter(family__location__catchments__health_facility=health_facility)\
                .filter(family__location__catchments__validity_to__isnull=True)\
                .annotate(sum=Sum(F('family__location__catchments__catchment'))*Count('id')/100)
        sum_policy = 0
        for policy in policies:
            sum_policy += policy.sum
        return sum_policy
    else:
        return 0


def get_product_sum_population(product):
    villages = get_product_villages(product)
    if villages is not None:
        pop = villages.annotate(sum_pop=Sum((F('male_population')+F('female_population')+F('other_population'))))\
                .annotate(sum_families=Sum((F('families'))))

        sum_pop, sum_families = 0, 0
        for p in pop:
            sum_pop += p.sum_pop
            sum_families += p.sum_families

        return sum_pop, sum_families
    else:
        return 0, 0


def get_product_sum_claim(product, start_date, end_date, health_facility=None):
    # make the items querysets
    items = ClaimItem.objects.filter(validity_to__isnull=True)\
        .filter(product=product)\
        .filter(claim__processed_date__lte=end_date)\
        .filter(claim__processed_date__gt=start_date)
    # make the services querysets
    services = ClaimService.objects.filter(validity_to__isnull=True)\
        .filter(product=product)\
        .filter(claim__processed_date__lte=end_date)\
        .filter(claim__processed_date__gt=start_date)
    # get the number of claims concened by the Items and services queryset
    if health_facility is not None:
        items = items.filter(claim__health_facility=health_facility)
        services = services.filter(claim__health_facility=health_facility)
    # count the distinct claims
    visits = items.only('claim').union(services.only('claim')).annotate(sum=Count('claim'))
    # addup all adjusted_amount
    items = items.annotate(sum=Sum('adjusted_amount'))
    services = services.annotate(sum=Sum('adjusted_amount'))

    sum_items, sum_services, sum_visits = 0, 0, 0
    for visit in visits:
        sum_visits += visit.sum
    for item in items:
        sum_items += item.sum
    for service in services:
        sum_services += service.sum

    return sum_items + sum_services, sum_visits
