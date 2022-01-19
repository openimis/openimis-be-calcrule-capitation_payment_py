import logging
from claim.models import ClaimItem, Claim, ClaimService
from django.db import connection
from django.db.models import Value
from django.db.models.functions import Coalesce
from django.contrib.contenttypes.models import ContentType
from invoice.models import Bill
from location.models import Location

logger = logging.getLogger(__name__)

@deprecated
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

@deprecated
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

@deprecated
def process_capitation_payment_data(params):
    with connection.cursor() as cur:
        # HFLevel based on
        # https://github.com/openimis/web_app_vb/blob/2492c20d8959e39775a2dd4013d2fda8feffd01c/IMIS_BL/HealthFacilityBL.vb#L77
        _execute_capitation_payment_procedure(cur, 'uspCreateCapitationPaymentReportData', params)

@deprecated
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

@deprecated
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


def check_bill_not_exist(instance, health_facility, **kwargs):
    if instance.__class__.__name__ == "BatchRun":
        batch_run = instance
        content_type = ContentType.objects.get_for_model(batch_run.__class__)
        bills = Bill.objects.filter(
            subject_type=content_type,
            subject_id=batch_run.id,
            thirdparty_id=health_facility.id
        )
        if bills.exists() == False:
            return True

def generate_capitation(product, start_date, end_date, allocated_contribution ):
    population_matter = product.weight_population > 0 or product.weight_nb_families > 0
    year = end_date.year
    month = end_date.month
    if ( product.weight_insured_population > 0 or product.weight_nb_insured_families > 0 or  population_matter ):
        # get location (district) linked to the product --> to be 
        sum_pop, sum_families = 1
        if are_population_matter:
            sum_pop, sum_families = get_product_sum_population(product)
        sum_insurees = 1
        # get the total number of insuree
        if (product.weight_insured_population > 0):
            insuree =  get_product_sum_insurees(product, start_date, end_date)
        sum_insured_family = 1
        # get the total number of insured family
        sum_insured_families = 1
        if (product.weight_nb_insured_families > 0):
            sum_insured_families = get_product_sum_policies(product, start_date, end_date)        
        #get the claim data 
        sum_claim_adjusted_amount, sum_visist =1
        if product.weight_nb_visits >0 or product.weight_adjusted_amount >0:
            sum_claim_adjusted_amount, sum_visits = get_product_sum_claim(product, start_date, end_date)
        # select HF concerned with capitation (new HF will come from claims)
        health_facilities = get_prodcut_hf_filter(product, get_product_health_facilites(product))\
            .select_related('location', queryset = Location.objects.filter(validity_to__isnull=True))\
            .select_related('location__parent', queryset = Location.objects.filter(validity_to__isnull=True))
        # create n capitaiton report for each facilits
        foreach health_facility in health_facilities:
            generate_capitation_health_facility(product, health_facility, allocatied_contribution, sum_insuree, sum_insured_families, 
            sum_pop, sum_families, sum_claim_adjusted_amount, sum_visits, year, month)


def get_hf_sum_population(health_facility):
    pop = Location.objects.filter(validity_to__isnull=True)\
            .filter(catchments__health_facility = health_facility)\
            .filter(catchments__validity_to__isnull=True)\
            .annotate(sum_pop=SUM((F('male_population')+F('female_population')+F('other_population'))*F('catchments__catchment')/100)
            .annotate(sum_families=SUM((F('male_population')+F('female_population')+F('other_population'))*F('catchments__catchment')/100)

    return pop['sum_pop'], pop['sum_families']

def get_prodcut_hf_filter(product, queryset):
    # takes all HF if not level config is defined (ie. no filter added)
    if (product.capitation_sublevel_1 is not None or \
        product.capitation_sublevel_2 is not None or \
        product.capitation_sublevel_3 is not None or \
        product.capitation_sublevel_4 is not None  ):
        # take the HF that match level and sublevel OR level if sublevel is not set in product
        queryset =queryset.filter((Q(HFLevel = product.capitation_level_1) &\
            (Q(HFSubLevel = product.capitation_sublevel_1) | Q(product.capitation_sublevel_1 is None))) |\
        (Q(HFLevel = product.capitation_level_2) &\
            (Q(HFSubLevel = product.capitation_sublevel_2) | Q(product.capitation_sublevel_2 is None))) |\
        (Q(HFLevel = product.capitation_level_3) &\
            (Q(HFSubLevel = product.capitation_sublevel_3) | Q(product.capitation_sublevel_3 is None))) |\
        (Q(HFLevel = product.capitation_level_4) &\
            (Q(HFSubLevel = product.capitation_sublevel_4) | Q(product.capitation_sublevel_4 is None))))
    return queryset

def generate_capitation_health_facility(product, health_facility, allocated_contribution, sum_insurees, sum_insured_families, 
            sum_pop, sum_families, sum_adjusted_amount, sum_visits, year, month):
    population_matter = product.weight_population > 0 or product.weight_nb_families > 0
    sum_hf_insuree, sum_hf_insured_familly = 0
    sum_hf_pop, sum_hf_families = 0
    # get the sum of pop
    if population_matter:
        sum_hf_pop, sum_hf_families = get_hf_sum_population(health_facility)
    # get the sum of insuree
    if (product.weight_insured_population > 0):
        sum_hf_insurees =   get_product_sum_insurees(product, start_date, end_date, health_facility)
    # get the sum of policy/insureed families
    if (product.weight_nb_insured_families > 0):
        sum_hf_insured_families =   get_product_sum_policies(product, start_date, end_date, health_facility)

    if product.weight_nb_visits >0 or product.weight_adjusted_amount >0:
        sum_hf_claim_adjusted_amount, sum_hf_visist = get_product_sum_claim(product, start_date, end_date, health_facility)
    # ammont available for all HF capitaiton
    allocated =  allocated_contribution * product.share_contribution /100
    # Alloacted ammount for the Prodcut (common for all HF)
    alc_contri_population = allocated * product.weight_population / 100 
    alc_contri_num_families = allocated *  product.weight_nb_families / 100
    alc_contri_ins_population = allocated *  product.weight_insured_population /100
    alc_contri_ins_families = allocated *  product.weight_nb_insured_families /100
    alc_contri_visits = allocated *  product.weight_nb_visits /100
    alc_contri_adjusted_amount =  allocated *  product.weight_adjusted_amount /100
    # unit  (common for all HF)
    up_population = alc_contri_population / sum_pop
    up_num_families = alc_contri_num_families / sum_families
    up_ins_population = alc_contri_ins_population / sum_insurees
    up_ins_families = alc_contri_ins_families / sum_insured_families
    up_visits = alc_contri_visits / sum_visits
    up_adjusted_amount =  aalc_contri_adjusted_amount /sum_adjusted_amount

    # amount for this HF
    total_population = sum_hf_pop * up_population
    total_families = sum_hf_families * up_num_families
    total_ins_population = sum_hf_insurees * up_ins_population
    total_ins_families = sum_hf_insured_families * up_ins_families
    total_claims = sum_hf_visist * up_visits
    total_adjusted = sum_hf_adjusted_amount * up_adjusted_amount

    # overall total
    payment_cathment = total_population + total_families\
                    +total_ins_population + total_ins_families

    # Create the CapitationPayment so it can be retrieved from the invocie to generate the legacy reports
    if payment_cathment > 0 :
        Capitation = new CapitationPayment( year = year,\
                        month = month,\
                        health_facility = health_facility,\
                        region_code = health_facility.location.parent.code,\
                        region_name = health_facility.location.parent.code,\
                        distric_code = health_facility.location.code,
                        district_name = health_facility.location.code,\
                        health_facility_code = health_facility.code,\
                        health_facility_name = health_facility.name,\
                        hf_level = health_facility.level,\
                        hf_sublevel = health_facility.sublevel,\
                        total_population = total_population,\
                        total_families = total_families,\
                        total_insured_insuree = total_insured_insuree,\
                        total_insured_families = total_insured_families,\
                        total_claims = total_claims,\
                        total_adjusted = total_adjusted ,\
                        alc_contri_population = alc_contri_population,\
                        alc_contri_num_families = alc_contri_num_families,\
                        alc_contri_ins_population = alc_contri_ins_population,\
                        alc_contri_ins_families = alc_contri_ins_families,\
                        payment_cathment = total_population + total_families\
                                            +total_ins_population + total_ins_families,\
                        up_population = up_population,\
                        up_num_families=up_num_families,\
                        up_ins_population=up_ins_population,\
                        up_ins_families=up_ins_families,\
                        up_visits=up_visits,\
                        up_adjusted_amount=up_adjusted_amount)
        # TODO create bill with Capitation in the json_ext_details                    
                                                  
                                            
                                        )



# below might  be move to Product Module

 def get_product_districts(product):
    districts = Location.objects.filer(validity_to__isnull=True)
     # if location null, it means all 
    if product.location is None:
        districts = districts.all()
    elif product.location.type = 'D':
        # ideally we should just return the object but the caller will expect a queryset not an object
        districts = districts.filter(id=product.location.id )
    elif product.location.type == 'R':
         districts = districts.filter(parent_id=product.location.id )
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
 
 def get_product_health_facilites(product):
    districts  = get_districts(product)
    if district is not None:
        health_facilities = get_prodcut_hf_filter(product, HealthFacility.objects.filter(validity_to__isnull=True)\
            .filter(location__in = districts))
        return health_facilities
    else:
        return None


 def get_product_sum_insurees(product, start_date, end_date, health_facility = None):
    villages = get_product_villages(product, start_date, end_date)
    if villages is not None:
        insurees  = InsureePolicy.objects.filter(validity_to__isnull=True)\
                    .filter(family__location__in= villages)\
                    .filter(policy_expiry_date_gte=start_date  )\
                    .filter(policy_effective_date_lte=start_date  )\
                    .filter(policy_product=product )
        # filter based on catchement if HF is defined
        if health_facility is None:
            insurees = insurees.annotate(sum=COUNT(id)/100)
        else:
            insurees = insurees.filter(policy__family__location__catchments__health_facility = health_facility)\
                .filter(policy__family__location__catchments__validity_to__isnull=True)\
                .annotate(sum=F('family__location__catchments__catchement') * COUNT(id) /100 )['sum']
        return insurees['sum']
    else:
        return 0

def get_product_sum_policies(product, start_date, end_date, health_facility = None):
    villages = get_product_villages(product)
    if villages is not None:
        policies  = Policy.objects.filter(validity_to__isnull=True)\
                    .filter(family__location__in= villages)\
                    .filter(expiry_date_gte=start_date  )\
                    .filter(effective_date_lte=start_date  )\
                    .filter(product=product )
        # filter based on catchement if HF is defined
        if health_facility is None:
                policies = policies.annotate(sum=COUNT(id)/100)
        else:
            policies = policies.filter(family__location__catchments__health_facility = health_facility)\
                .filter(family__location__catchments__validity_to__isnull=True)\
                .annotate(sum=F('family__location__catchments__catchement') * COUNT(id) /100 )
        return policies['sum']
    else:
        return 0

def get_product_sum_population(product):
    villages = get_product_villages(product)
    if villages is not None:    
        pop = villages.annotate(sum_pop=SUM((F('male_population')+F('female_population')+F('other_population')))
                .annotate(sum_families=SUM((F('families'))))

        return pop['sum_pop'], pop['sum_families']
    else:
        return 0, 0

def get_product_sum_claim(product, start_date, end_date, health_facility = None):
    # make the items querysets
    items = ClaimsItems.objects.filter(validity_to__isnull=True)\
        .filter(product = product)\
        .filter(claim__processed_date__lte=end_date)\
        .filter(claim__processed_date__gt=start_date)
    # make the services querysets
    services = ClaimsServices.objects.filter(validity_to__isnull=True)\
        .filter(product = product)\
        .filter(claim__processed_date__lte=end_date)\
        .filter(claim__processed_date__gt=start_date)
    # get the number of claims concened by the Items and services queryset
    if health_facility is not None:
        items = items.filter(claim__health_facility = health_facility)
        services = services.filter(claim__health_facility = health_facility)
    # count the distinct claims
    visits = items.only('claim').union(services.only('claim')).annotate(sum=COUNT(claim))
    # addup all adjusted_amount
    items = items.annotate(sum=SUM('adjusted_amount'))\
    services = services.annotate(sum=SUM('adjusted_amount'))

    return items['sum'] + services['sum'], visits['sum']

        
