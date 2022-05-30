import logging

from location.models import Location


logger = logging.getLogger(__name__)


def get_capitation_region_and_district(location_id):
    if not location_id:
        return None, None, None, None
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
