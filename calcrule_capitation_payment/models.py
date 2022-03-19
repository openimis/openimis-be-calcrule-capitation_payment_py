import uuid
from django.db import models

from core import models as core_models
from location.models import HealthFacility
from product.models import Product


# TODO no tables
class CapitationPayment(core_models.VersionedModel):
    id = models.AutoField(db_column='CapitationPaymentID', primary_key=True)
    uuid = models.CharField(db_column='CapitationPaymentUUID', max_length=36, default=uuid.uuid4, unique=True)

    year = models.IntegerField('Year', null=False)
    month = models.IntegerField('Month', null=False)
    product = models.ForeignKey(Product, models.DO_NOTHING, db_column='ProductID',
                                related_name="capitation_payment_product")

    health_facility = models.ForeignKey(HealthFacility, models.DO_NOTHING, db_column='HfID',
                                        related_name="capitation_payment_health_facility")

    region_code = models.CharField(db_column='RegionCode', max_length=8, null=True, blank=True)
    region_name = models.CharField(db_column='RegionName', max_length=50, null=True, blank=True)

    district_code = models.CharField(db_column='DistrictCode', max_length=8, null=True, blank=True)
    district_name = models.CharField(db_column='DistrictName', max_length=50, null=True, blank=True)

    health_facility_code = models.CharField(db_column='HFCode', max_length=8)
    health_facility_name = models.CharField(db_column='HFName', max_length=100)

    acc_code = models.CharField(db_column='AccCode', max_length=25, null=True, blank=True)

    hf_level = models.CharField(db_column='HFLevel', max_length=100, blank=True, null=True)
    hf_sublevel = models.CharField(db_column='HFSublevel', max_length=100, blank=True, null=True)

    total_population = models.DecimalField(
        db_column='TotalPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_families = models.DecimalField(
        db_column='TotalFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_insured_insuree = models.DecimalField(
        db_column='TotalInsuredInsuree', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_insured_families = models.DecimalField(
        db_column='TotalInsuredFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_claims = models.DecimalField(
        db_column='TotalClaims', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_population = models.DecimalField(
        db_column='AlcContriPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_num_families = models.DecimalField(
        db_column='AlcContriNumFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_ins_population = models.DecimalField(
        db_column='AlcContriInsPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_ins_families = models.DecimalField(
        db_column='AlcContriInsFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_visits = models.DecimalField(
        db_column='AlcContriVisits', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_adjusted_amount = models.DecimalField(
        db_column='AlcContriAdjustedAmount', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_population = models.DecimalField(
        db_column='UPPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_num_families = models.DecimalField(
        db_column='UPNumFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_ins_population = models.DecimalField(
        db_column='UPInsPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_ins_families = models.DecimalField(
        db_column='UPInsFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_visits = models.DecimalField(
        db_column='UPVisits', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_adjusted_amount = models.DecimalField(
        db_column='UPAdjustedAmount', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    payment_cathment = models.DecimalField(
        db_column='PaymentCathment', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_adjusted = models.DecimalField(
        db_column='TotalAdjusted', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    class Meta:
        db_table = 'tblCapitationPayment'
