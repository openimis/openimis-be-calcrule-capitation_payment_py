import logging
from claim.models import ClaimItem, Claim, ClaimService
from django.db import connection
from django.db.models import Value
from django.db.models.functions import Coalesce
from django.contrib.contenttypes.models import ContentType
from invoice.models import Bill
from location.models import Location

logger = logging.getLogger(__name__)


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


def process_capitation_payment_data(params):
    with connection.cursor() as cur:
        # HFLevel based on
        # https://github.com/openimis/web_app_vb/blob/2492c20d8959e39775a2dd4013d2fda8feffd01c/IMIS_BL/HealthFacilityBL.vb#L77
        _execute_capitation_payment_procedure(cur, 'uspCreateCapitationPaymentReportData', params)


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
