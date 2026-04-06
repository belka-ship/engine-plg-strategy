#!/usr/bin/env python3
"""Fetch all NYC hotel static data from DOTW API and save as JSON."""

import requests
import xml.etree.ElementTree as ET
import json
import sys

USERNAME = "GreenTurtleVentures"
PASSWORD = "19fcb31231ba24b37efd32379fc60e62"
CUSTOMER_ID = "2167845"
API_URL = "https://xmldev.dotwconnect.com/gatewayV4.dotw"

def fetch_hotels():
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<customer>
    <username>{USERNAME}</username>
    <password>{PASSWORD}</password>
    <id>{CUSTOMER_ID}</id>
    <source>1</source>
    <product>hotel</product>
    <language>en</language>
    <request command="searchhotels">
        <bookingDetails>
            <fromDate>2026-06-01</fromDate>
            <toDate>2026-06-03</toDate>
            <currency>520</currency>
            <rooms no="1">
                <room runno="0">
                    <adultsCode>2</adultsCode>
                    <children no="0"></children>
                    <rateBasis>-1</rateBasis>
                    <passengerNationality>102</passengerNationality>
                    <passengerCountryOfResidence>102</passengerCountryOfResidence>
                </room>
            </rooms>
        </bookingDetails>
        <return>
            <getRooms>true</getRooms>
            <filters xmlns:a="http://us.dotwconnect.com/xsd/atomicCondition"
                     xmlns:c="http://us.dotwconnect.com/xsd/complexCondition">
                <city>17144</city>
                <noPrice>true</noPrice>
            </filters>
            <fields>
                <field>preferred</field>
                <field>builtYear</field>
                <field>renovationYear</field>
                <field>floors</field>
                <field>noOfRooms</field>
                <field>fullAddress</field>
                <field>description1</field>
                <field>description2</field>
                <field>hotelName</field>
                <field>address</field>
                <field>zipCode</field>
                <field>location</field>
                <field>locationId</field>
                <field>geoLocations</field>
                <field>location1</field>
                <field>location2</field>
                <field>location3</field>
                <field>cityName</field>
                <field>cityCode</field>
                <field>stateName</field>
                <field>stateCode</field>
                <field>countryName</field>
                <field>countryCode</field>
                <field>regionName</field>
                <field>regionCode</field>
                <field>attraction</field>
                <field>amenitie</field>
                <field>leisure</field>
                <field>business</field>
                <field>transportation</field>
                <field>hotelPhone</field>
                <field>hotelCheckIn</field>
                <field>hotelCheckOut</field>
                <field>minAge</field>
                <field>rating</field>
                <field>images</field>
                <field>fireSafety</field>
                <field>hotelPreference</field>
                <field>direct</field>
                <field>geoPoint</field>
                <field>leftToSell</field>
                <field>chain</field>
                <field>lastUpdated</field>
                <field>priority</field>
                <roomField>name</roomField>
                <roomField>roomInfo</roomField>
                <roomField>roomAmenities</roomField>
                <roomField>twin</roomField>
            </fields>
        </return>
    </request>
</customer>"""

    print("Fetching NYC hotels from DOTW API...")
    headers = {
        "Content-Type": "text/xml",
        "Accept-Encoding": "gzip",
        "Connection": "close"
    }
    response = requests.post(API_URL, data=xml, headers=headers)
    print(f"Response status: {response.status_code}, length: {len(response.text)} chars")
    return response.text


def elem_text(el, tag):
    """Get text of a child element, or None."""
    child = el.find(tag)
    return child.text if child is not None and child.text else None


def elem_all_text(el, tag):
    """Get text of all matching child elements as a list."""
    return [c.text for c in el.findall(tag) if c.text]


def parse_items(el, tag):
    """Parse <tag><item>...</item></tag> into a list of strings."""
    container = el.find(tag)
    if container is None:
        return []
    return [item.text for item in container.findall('item') if item.text]


def parse_images(el):
    """Parse image elements into list of dicts."""
    images = []
    for img in el.findall('.//image'):
        image_data = {}
        for child in img:
            if child.text:
                image_data[child.tag] = child.text
        if image_data:
            images.append(image_data)
    return images


def parse_rooms(hotel_el):
    """Parse room types from hotel element."""
    rooms = []
    for room_el in hotel_el.findall('.//rooms/room'):
        adults = room_el.get('adults')
        children = room_el.get('children')
        for rt in room_el.findall('roomType'):
            room = {
                'roomTypeCode': rt.get('roomtypecode'),
                'name': elem_text(rt, 'name'),
                'adults': adults,
                'children': children,
            }
            # roomInfo
            info = elem_text(rt, 'roomInfo')
            if info:
                room['roomInfo'] = info
            # roomAmenities
            amenities_el = rt.find('roomAmenities')
            if amenities_el is not None:
                room['roomAmenities'] = [a.text for a in amenities_el if a.text]
            # twin
            twin = elem_text(rt, 'twin')
            if twin:
                room['twin'] = twin
            rooms.append(room)
    return rooms


def parse_geo(hotel_el):
    """Parse geoPoint."""
    geo = hotel_el.find('geoPoint')
    if geo is None:
        return None
    lat = elem_text(geo, 'lat')
    lng = elem_text(geo, 'lng')
    if lat and lng:
        return {'lat': float(lat), 'lng': float(lng)}
    return None


RATING_MAP = {
    '559': 'Economy *',
    '560': 'Budget **',
    '561': 'Standard ***',
    '562': 'Superior ****',
    '563': 'Luxury *****',
    '48055': 'Serviced Apartment',
    '55835': 'Unrated',
}


def parse_hotels(xml_text):
    root = ET.fromstring(xml_text)

    successful = root.find('.//successful')
    if successful is None or successful.text != 'TRUE':
        print("API call was not successful!")
        # Print error if any
        error = root.find('.//error/details')
        if error is not None:
            print(f"Error: {error.text}")
        print("First 2000 chars of response:")
        print(xml_text[:2000])
        return []

    hotels = []
    for h in root.findall('.//hotel'):
        hotel = {
            'hotelId': h.get('hotelid'),
            'hotelName': elem_text(h, 'hotelName'),
            'address': elem_text(h, 'address'),
            'fullAddress': elem_text(h, 'fullAddress'),
            'zipCode': elem_text(h, 'zipCode'),
            'description1': elem_text(h, 'description1'),
            'description2': elem_text(h, 'description2'),
            'ratingCode': elem_text(h, 'rating'),
            'rating': RATING_MAP.get(elem_text(h, 'rating') or '', elem_text(h, 'rating')),
            'geoPoint': parse_geo(h),
            'location': elem_text(h, 'location'),
            'locationId': elem_text(h, 'locationId'),
            'location1': elem_text(h, 'location1'),
            'location2': elem_text(h, 'location2'),
            'location3': elem_text(h, 'location3'),
            'cityName': elem_text(h, 'cityName'),
            'cityCode': elem_text(h, 'cityCode'),
            'stateName': elem_text(h, 'stateName'),
            'stateCode': elem_text(h, 'stateCode'),
            'countryName': elem_text(h, 'countryName'),
            'countryCode': elem_text(h, 'countryCode'),
            'regionName': elem_text(h, 'regionName'),
            'regionCode': elem_text(h, 'regionCode'),
            'hotelPhone': elem_text(h, 'hotelPhone'),
            'hotelCheckIn': elem_text(h, 'hotelCheckIn'),
            'hotelCheckOut': elem_text(h, 'hotelCheckOut'),
            'minAge': elem_text(h, 'minAge'),
            'chain': elem_text(h, 'chain'),
            'preferred': elem_text(h, 'preferred'),
            'builtYear': elem_text(h, 'builtYear'),
            'renovationYear': elem_text(h, 'renovationYear'),
            'floors': elem_text(h, 'floors'),
            'noOfRooms': elem_text(h, 'noOfRooms'),
            'direct': elem_text(h, 'direct'),
            'hotelPreference': elem_text(h, 'hotelPreference'),
            'leftToSell': elem_text(h, 'leftToSell'),
            'lastUpdated': elem_text(h, 'lastUpdated'),
            'priority': elem_text(h, 'priority'),
        }

        # Amenities, leisure, business, transportation, fireSafety, attraction
        for list_field in ['amenitie', 'leisure', 'business', 'transportation', 'fireSafety', 'attraction']:
            container = h.find(list_field)
            if container is not None:
                items = [item.text for item in container if item.text]
                if items:
                    hotel[list_field] = items

        # Images
        imgs = parse_images(h)
        if imgs:
            hotel['images'] = imgs

        # Rooms
        rooms = parse_rooms(h)
        if rooms:
            hotel['rooms'] = rooms

        # geoLocations
        geo_locs = h.find('geoLocations')
        if geo_locs is not None:
            locs = []
            for loc in geo_locs:
                loc_data = {}
                for child in loc:
                    if child.text:
                        loc_data[child.tag] = child.text
                if loc_data:
                    locs.append(loc_data)
            if locs:
                hotel['geoLocations'] = locs

        # Remove None values for cleaner output
        hotel = {k: v for k, v in hotel.items() if v is not None}

        hotels.append(hotel)

    return hotels


def main():
    xml_response = fetch_hotels()
    hotels = parse_hotels(xml_response)
    print(f"\nParsed {len(hotels)} hotels")

    if not hotels:
        print("No hotels found. Saving raw response for debugging.")
        with open('data/nyc_hotels_raw.xml', 'w') as f:
            f.write(xml_response)
        return

    # Save as JSON
    output_path = 'data/nyc_hotels.json'
    with open(output_path, 'w') as f:
        json.dump(hotels, f, indent=2, ensure_ascii=False)
    print(f"Saved to {output_path}")

    # Print summary
    ratings = {}
    for h in hotels:
        r = h.get('rating', 'Unknown')
        ratings[r] = ratings.get(r, 0) + 1
    print("\nBy rating:")
    for r, count in sorted(ratings.items()):
        print(f"  {r}: {count}")

    # Sample hotel
    print(f"\nSample hotel: {hotels[0].get('hotelName')} (ID: {hotels[0].get('hotelId')})")
    print(f"  Address: {hotels[0].get('address')}")
    print(f"  Rating: {hotels[0].get('rating')}")
    if 'rooms' in hotels[0]:
        print(f"  Room types: {len(hotels[0]['rooms'])}")


if __name__ == '__main__':
    main()
