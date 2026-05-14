#!/usr/bin/env python3
from __future__ import annotations

import os
import yt.wrapper as yt

YT_PROXY = os.environ.get("YT_PROXY", "http://localhost:31103").replace("http://", "").replace("https://", "").rstrip("/")
YT_TOKEN = os.environ.get("YT_TOKEN")
REF_PATH = os.environ.get("COUNTRY_REF_PATH", "//home/ref/iso_dim")
SILVER_PATH = os.environ.get("SILVER_PATH", "//home/silver_stage/greenhub_telemetry")

ISO_SCHEMA = [
    {"name": "country_code", "type": "string"},
    {"name": "country_name", "type": "string"},
    {"name": "alpha3_code", "type": "string"},
    {"name": "numeric_code", "type": "string"},
    {"name": "official_name", "type": "string"},
    {"name": "region", "type": "string"},
    {"name": "sub_region", "type": "string"},
]

# ISO 3166-1 countries/territories generated from pycountry/ISO 3166 data.
# region/sub_region are left empty intentionally; they can be populated later from a UN M49 reference.
ISO_ROWS = [
  {
    "country_code": "AD",
    "country_name": "Andorra",
    "alpha3_code": "AND",
    "numeric_code": "020",
    "official_name": "Principality of Andorra",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AE",
    "country_name": "United Arab Emirates",
    "alpha3_code": "ARE",
    "numeric_code": "784",
    "official_name": "United Arab Emirates",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AF",
    "country_name": "Afghanistan",
    "alpha3_code": "AFG",
    "numeric_code": "004",
    "official_name": "Islamic Republic of Afghanistan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AG",
    "country_name": "Antigua and Barbuda",
    "alpha3_code": "ATG",
    "numeric_code": "028",
    "official_name": "Antigua and Barbuda",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AI",
    "country_name": "Anguilla",
    "alpha3_code": "AIA",
    "numeric_code": "660",
    "official_name": "Anguilla",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AL",
    "country_name": "Albania",
    "alpha3_code": "ALB",
    "numeric_code": "008",
    "official_name": "Republic of Albania",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AM",
    "country_name": "Armenia",
    "alpha3_code": "ARM",
    "numeric_code": "051",
    "official_name": "Republic of Armenia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AO",
    "country_name": "Angola",
    "alpha3_code": "AGO",
    "numeric_code": "024",
    "official_name": "Republic of Angola",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AQ",
    "country_name": "Antarctica",
    "alpha3_code": "ATA",
    "numeric_code": "010",
    "official_name": "Antarctica",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AR",
    "country_name": "Argentina",
    "alpha3_code": "ARG",
    "numeric_code": "032",
    "official_name": "Argentine Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AS",
    "country_name": "American Samoa",
    "alpha3_code": "ASM",
    "numeric_code": "016",
    "official_name": "American Samoa",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AT",
    "country_name": "Austria",
    "alpha3_code": "AUT",
    "numeric_code": "040",
    "official_name": "Republic of Austria",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AU",
    "country_name": "Australia",
    "alpha3_code": "AUS",
    "numeric_code": "036",
    "official_name": "Australia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AW",
    "country_name": "Aruba",
    "alpha3_code": "ABW",
    "numeric_code": "533",
    "official_name": "Aruba",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AX",
    "country_name": "Åland Islands",
    "alpha3_code": "ALA",
    "numeric_code": "248",
    "official_name": "Åland Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "AZ",
    "country_name": "Azerbaijan",
    "alpha3_code": "AZE",
    "numeric_code": "031",
    "official_name": "Republic of Azerbaijan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BA",
    "country_name": "Bosnia and Herzegovina",
    "alpha3_code": "BIH",
    "numeric_code": "070",
    "official_name": "Republic of Bosnia and Herzegovina",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BB",
    "country_name": "Barbados",
    "alpha3_code": "BRB",
    "numeric_code": "052",
    "official_name": "Barbados",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BD",
    "country_name": "Bangladesh",
    "alpha3_code": "BGD",
    "numeric_code": "050",
    "official_name": "People's Republic of Bangladesh",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BE",
    "country_name": "Belgium",
    "alpha3_code": "BEL",
    "numeric_code": "056",
    "official_name": "Kingdom of Belgium",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BF",
    "country_name": "Burkina Faso",
    "alpha3_code": "BFA",
    "numeric_code": "854",
    "official_name": "Burkina Faso",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BG",
    "country_name": "Bulgaria",
    "alpha3_code": "BGR",
    "numeric_code": "100",
    "official_name": "Republic of Bulgaria",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BH",
    "country_name": "Bahrain",
    "alpha3_code": "BHR",
    "numeric_code": "048",
    "official_name": "Kingdom of Bahrain",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BI",
    "country_name": "Burundi",
    "alpha3_code": "BDI",
    "numeric_code": "108",
    "official_name": "Republic of Burundi",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BJ",
    "country_name": "Benin",
    "alpha3_code": "BEN",
    "numeric_code": "204",
    "official_name": "Republic of Benin",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BL",
    "country_name": "Saint Barthélemy",
    "alpha3_code": "BLM",
    "numeric_code": "652",
    "official_name": "Saint Barthélemy",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BM",
    "country_name": "Bermuda",
    "alpha3_code": "BMU",
    "numeric_code": "060",
    "official_name": "Bermuda",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BN",
    "country_name": "Brunei Darussalam",
    "alpha3_code": "BRN",
    "numeric_code": "096",
    "official_name": "Brunei Darussalam",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BO",
    "country_name": "Bolivia, Plurinational State of",
    "alpha3_code": "BOL",
    "numeric_code": "068",
    "official_name": "Plurinational State of Bolivia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BQ",
    "country_name": "Bonaire, Sint Eustatius and Saba",
    "alpha3_code": "BES",
    "numeric_code": "535",
    "official_name": "Bonaire, Sint Eustatius and Saba",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BR",
    "country_name": "Brazil",
    "alpha3_code": "BRA",
    "numeric_code": "076",
    "official_name": "Federative Republic of Brazil",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BS",
    "country_name": "Bahamas",
    "alpha3_code": "BHS",
    "numeric_code": "044",
    "official_name": "Commonwealth of the Bahamas",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BT",
    "country_name": "Bhutan",
    "alpha3_code": "BTN",
    "numeric_code": "064",
    "official_name": "Kingdom of Bhutan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BV",
    "country_name": "Bouvet Island",
    "alpha3_code": "BVT",
    "numeric_code": "074",
    "official_name": "Bouvet Island",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BW",
    "country_name": "Botswana",
    "alpha3_code": "BWA",
    "numeric_code": "072",
    "official_name": "Republic of Botswana",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BY",
    "country_name": "Belarus",
    "alpha3_code": "BLR",
    "numeric_code": "112",
    "official_name": "Republic of Belarus",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "BZ",
    "country_name": "Belize",
    "alpha3_code": "BLZ",
    "numeric_code": "084",
    "official_name": "Belize",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CA",
    "country_name": "Canada",
    "alpha3_code": "CAN",
    "numeric_code": "124",
    "official_name": "Canada",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CC",
    "country_name": "Cocos (Keeling) Islands",
    "alpha3_code": "CCK",
    "numeric_code": "166",
    "official_name": "Cocos (Keeling) Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CD",
    "country_name": "Congo, The Democratic Republic of the",
    "alpha3_code": "COD",
    "numeric_code": "180",
    "official_name": "Congo, The Democratic Republic of the",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CF",
    "country_name": "Central African Republic",
    "alpha3_code": "CAF",
    "numeric_code": "140",
    "official_name": "Central African Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CG",
    "country_name": "Congo",
    "alpha3_code": "COG",
    "numeric_code": "178",
    "official_name": "Republic of the Congo",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CH",
    "country_name": "Switzerland",
    "alpha3_code": "CHE",
    "numeric_code": "756",
    "official_name": "Swiss Confederation",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CI",
    "country_name": "Côte d'Ivoire",
    "alpha3_code": "CIV",
    "numeric_code": "384",
    "official_name": "Republic of Côte d'Ivoire",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CK",
    "country_name": "Cook Islands",
    "alpha3_code": "COK",
    "numeric_code": "184",
    "official_name": "Cook Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CL",
    "country_name": "Chile",
    "alpha3_code": "CHL",
    "numeric_code": "152",
    "official_name": "Republic of Chile",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CM",
    "country_name": "Cameroon",
    "alpha3_code": "CMR",
    "numeric_code": "120",
    "official_name": "Republic of Cameroon",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CN",
    "country_name": "China",
    "alpha3_code": "CHN",
    "numeric_code": "156",
    "official_name": "People's Republic of China",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CO",
    "country_name": "Colombia",
    "alpha3_code": "COL",
    "numeric_code": "170",
    "official_name": "Republic of Colombia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CR",
    "country_name": "Costa Rica",
    "alpha3_code": "CRI",
    "numeric_code": "188",
    "official_name": "Republic of Costa Rica",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CU",
    "country_name": "Cuba",
    "alpha3_code": "CUB",
    "numeric_code": "192",
    "official_name": "Republic of Cuba",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CV",
    "country_name": "Cabo Verde",
    "alpha3_code": "CPV",
    "numeric_code": "132",
    "official_name": "Republic of Cabo Verde",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CW",
    "country_name": "Curaçao",
    "alpha3_code": "CUW",
    "numeric_code": "531",
    "official_name": "Curaçao",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CX",
    "country_name": "Christmas Island",
    "alpha3_code": "CXR",
    "numeric_code": "162",
    "official_name": "Christmas Island",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CY",
    "country_name": "Cyprus",
    "alpha3_code": "CYP",
    "numeric_code": "196",
    "official_name": "Republic of Cyprus",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "CZ",
    "country_name": "Czechia",
    "alpha3_code": "CZE",
    "numeric_code": "203",
    "official_name": "Czech Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "DE",
    "country_name": "Germany",
    "alpha3_code": "DEU",
    "numeric_code": "276",
    "official_name": "Federal Republic of Germany",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "DJ",
    "country_name": "Djibouti",
    "alpha3_code": "DJI",
    "numeric_code": "262",
    "official_name": "Republic of Djibouti",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "DK",
    "country_name": "Denmark",
    "alpha3_code": "DNK",
    "numeric_code": "208",
    "official_name": "Kingdom of Denmark",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "DM",
    "country_name": "Dominica",
    "alpha3_code": "DMA",
    "numeric_code": "212",
    "official_name": "Commonwealth of Dominica",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "DO",
    "country_name": "Dominican Republic",
    "alpha3_code": "DOM",
    "numeric_code": "214",
    "official_name": "Dominican Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "DZ",
    "country_name": "Algeria",
    "alpha3_code": "DZA",
    "numeric_code": "012",
    "official_name": "People's Democratic Republic of Algeria",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "EC",
    "country_name": "Ecuador",
    "alpha3_code": "ECU",
    "numeric_code": "218",
    "official_name": "Republic of Ecuador",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "EE",
    "country_name": "Estonia",
    "alpha3_code": "EST",
    "numeric_code": "233",
    "official_name": "Republic of Estonia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "EG",
    "country_name": "Egypt",
    "alpha3_code": "EGY",
    "numeric_code": "818",
    "official_name": "Arab Republic of Egypt",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "EH",
    "country_name": "Western Sahara",
    "alpha3_code": "ESH",
    "numeric_code": "732",
    "official_name": "Western Sahara",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ER",
    "country_name": "Eritrea",
    "alpha3_code": "ERI",
    "numeric_code": "232",
    "official_name": "the State of Eritrea",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ES",
    "country_name": "Spain",
    "alpha3_code": "ESP",
    "numeric_code": "724",
    "official_name": "Kingdom of Spain",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ET",
    "country_name": "Ethiopia",
    "alpha3_code": "ETH",
    "numeric_code": "231",
    "official_name": "Federal Democratic Republic of Ethiopia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "FI",
    "country_name": "Finland",
    "alpha3_code": "FIN",
    "numeric_code": "246",
    "official_name": "Republic of Finland",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "FJ",
    "country_name": "Fiji",
    "alpha3_code": "FJI",
    "numeric_code": "242",
    "official_name": "Republic of Fiji",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "FK",
    "country_name": "Falkland Islands (Malvinas)",
    "alpha3_code": "FLK",
    "numeric_code": "238",
    "official_name": "Falkland Islands (Malvinas)",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "FM",
    "country_name": "Micronesia, Federated States of",
    "alpha3_code": "FSM",
    "numeric_code": "583",
    "official_name": "Federated States of Micronesia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "FO",
    "country_name": "Faroe Islands",
    "alpha3_code": "FRO",
    "numeric_code": "234",
    "official_name": "Faroe Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "FR",
    "country_name": "France",
    "alpha3_code": "FRA",
    "numeric_code": "250",
    "official_name": "French Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GA",
    "country_name": "Gabon",
    "alpha3_code": "GAB",
    "numeric_code": "266",
    "official_name": "Gabonese Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GB",
    "country_name": "United Kingdom",
    "alpha3_code": "GBR",
    "numeric_code": "826",
    "official_name": "United Kingdom of Great Britain and Northern Ireland",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GD",
    "country_name": "Grenada",
    "alpha3_code": "GRD",
    "numeric_code": "308",
    "official_name": "Grenada",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GE",
    "country_name": "Georgia",
    "alpha3_code": "GEO",
    "numeric_code": "268",
    "official_name": "Georgia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GF",
    "country_name": "French Guiana",
    "alpha3_code": "GUF",
    "numeric_code": "254",
    "official_name": "French Guiana",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GG",
    "country_name": "Guernsey",
    "alpha3_code": "GGY",
    "numeric_code": "831",
    "official_name": "Guernsey",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GH",
    "country_name": "Ghana",
    "alpha3_code": "GHA",
    "numeric_code": "288",
    "official_name": "Republic of Ghana",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GI",
    "country_name": "Gibraltar",
    "alpha3_code": "GIB",
    "numeric_code": "292",
    "official_name": "Gibraltar",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GL",
    "country_name": "Greenland",
    "alpha3_code": "GRL",
    "numeric_code": "304",
    "official_name": "Greenland",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GM",
    "country_name": "Gambia",
    "alpha3_code": "GMB",
    "numeric_code": "270",
    "official_name": "Republic of the Gambia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GN",
    "country_name": "Guinea",
    "alpha3_code": "GIN",
    "numeric_code": "324",
    "official_name": "Republic of Guinea",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GP",
    "country_name": "Guadeloupe",
    "alpha3_code": "GLP",
    "numeric_code": "312",
    "official_name": "Guadeloupe",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GQ",
    "country_name": "Equatorial Guinea",
    "alpha3_code": "GNQ",
    "numeric_code": "226",
    "official_name": "Republic of Equatorial Guinea",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GR",
    "country_name": "Greece",
    "alpha3_code": "GRC",
    "numeric_code": "300",
    "official_name": "Hellenic Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GS",
    "country_name": "South Georgia and the South Sandwich Islands",
    "alpha3_code": "SGS",
    "numeric_code": "239",
    "official_name": "South Georgia and the South Sandwich Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GT",
    "country_name": "Guatemala",
    "alpha3_code": "GTM",
    "numeric_code": "320",
    "official_name": "Republic of Guatemala",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GU",
    "country_name": "Guam",
    "alpha3_code": "GUM",
    "numeric_code": "316",
    "official_name": "Guam",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GW",
    "country_name": "Guinea-Bissau",
    "alpha3_code": "GNB",
    "numeric_code": "624",
    "official_name": "Republic of Guinea-Bissau",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "GY",
    "country_name": "Guyana",
    "alpha3_code": "GUY",
    "numeric_code": "328",
    "official_name": "Republic of Guyana",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "HK",
    "country_name": "Hong Kong",
    "alpha3_code": "HKG",
    "numeric_code": "344",
    "official_name": "Hong Kong Special Administrative Region of China",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "HM",
    "country_name": "Heard Island and McDonald Islands",
    "alpha3_code": "HMD",
    "numeric_code": "334",
    "official_name": "Heard Island and McDonald Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "HN",
    "country_name": "Honduras",
    "alpha3_code": "HND",
    "numeric_code": "340",
    "official_name": "Republic of Honduras",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "HR",
    "country_name": "Croatia",
    "alpha3_code": "HRV",
    "numeric_code": "191",
    "official_name": "Republic of Croatia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "HT",
    "country_name": "Haiti",
    "alpha3_code": "HTI",
    "numeric_code": "332",
    "official_name": "Republic of Haiti",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "HU",
    "country_name": "Hungary",
    "alpha3_code": "HUN",
    "numeric_code": "348",
    "official_name": "Hungary",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ID",
    "country_name": "Indonesia",
    "alpha3_code": "IDN",
    "numeric_code": "360",
    "official_name": "Republic of Indonesia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IE",
    "country_name": "Ireland",
    "alpha3_code": "IRL",
    "numeric_code": "372",
    "official_name": "Ireland",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IL",
    "country_name": "Israel",
    "alpha3_code": "ISR",
    "numeric_code": "376",
    "official_name": "State of Israel",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IM",
    "country_name": "Isle of Man",
    "alpha3_code": "IMN",
    "numeric_code": "833",
    "official_name": "Isle of Man",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IN",
    "country_name": "India",
    "alpha3_code": "IND",
    "numeric_code": "356",
    "official_name": "Republic of India",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IO",
    "country_name": "British Indian Ocean Territory",
    "alpha3_code": "IOT",
    "numeric_code": "086",
    "official_name": "British Indian Ocean Territory",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IQ",
    "country_name": "Iraq",
    "alpha3_code": "IRQ",
    "numeric_code": "368",
    "official_name": "Republic of Iraq",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IR",
    "country_name": "Iran, Islamic Republic of",
    "alpha3_code": "IRN",
    "numeric_code": "364",
    "official_name": "Islamic Republic of Iran",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IS",
    "country_name": "Iceland",
    "alpha3_code": "ISL",
    "numeric_code": "352",
    "official_name": "Republic of Iceland",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "IT",
    "country_name": "Italy",
    "alpha3_code": "ITA",
    "numeric_code": "380",
    "official_name": "Italian Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "JE",
    "country_name": "Jersey",
    "alpha3_code": "JEY",
    "numeric_code": "832",
    "official_name": "Jersey",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "JM",
    "country_name": "Jamaica",
    "alpha3_code": "JAM",
    "numeric_code": "388",
    "official_name": "Jamaica",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "JO",
    "country_name": "Jordan",
    "alpha3_code": "JOR",
    "numeric_code": "400",
    "official_name": "Hashemite Kingdom of Jordan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "JP",
    "country_name": "Japan",
    "alpha3_code": "JPN",
    "numeric_code": "392",
    "official_name": "Japan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KE",
    "country_name": "Kenya",
    "alpha3_code": "KEN",
    "numeric_code": "404",
    "official_name": "Republic of Kenya",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KG",
    "country_name": "Kyrgyzstan",
    "alpha3_code": "KGZ",
    "numeric_code": "417",
    "official_name": "Kyrgyz Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KH",
    "country_name": "Cambodia",
    "alpha3_code": "KHM",
    "numeric_code": "116",
    "official_name": "Kingdom of Cambodia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KI",
    "country_name": "Kiribati",
    "alpha3_code": "KIR",
    "numeric_code": "296",
    "official_name": "Republic of Kiribati",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KM",
    "country_name": "Comoros",
    "alpha3_code": "COM",
    "numeric_code": "174",
    "official_name": "Union of the Comoros",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KN",
    "country_name": "Saint Kitts and Nevis",
    "alpha3_code": "KNA",
    "numeric_code": "659",
    "official_name": "Saint Kitts and Nevis",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KP",
    "country_name": "Korea, Democratic People's Republic of",
    "alpha3_code": "PRK",
    "numeric_code": "408",
    "official_name": "Democratic People's Republic of Korea",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KR",
    "country_name": "Korea, Republic of",
    "alpha3_code": "KOR",
    "numeric_code": "410",
    "official_name": "Korea, Republic of",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KW",
    "country_name": "Kuwait",
    "alpha3_code": "KWT",
    "numeric_code": "414",
    "official_name": "State of Kuwait",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KY",
    "country_name": "Cayman Islands",
    "alpha3_code": "CYM",
    "numeric_code": "136",
    "official_name": "Cayman Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "KZ",
    "country_name": "Kazakhstan",
    "alpha3_code": "KAZ",
    "numeric_code": "398",
    "official_name": "Republic of Kazakhstan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LA",
    "country_name": "Lao People's Democratic Republic",
    "alpha3_code": "LAO",
    "numeric_code": "418",
    "official_name": "Lao People's Democratic Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LB",
    "country_name": "Lebanon",
    "alpha3_code": "LBN",
    "numeric_code": "422",
    "official_name": "Lebanese Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LC",
    "country_name": "Saint Lucia",
    "alpha3_code": "LCA",
    "numeric_code": "662",
    "official_name": "Saint Lucia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LI",
    "country_name": "Liechtenstein",
    "alpha3_code": "LIE",
    "numeric_code": "438",
    "official_name": "Principality of Liechtenstein",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LK",
    "country_name": "Sri Lanka",
    "alpha3_code": "LKA",
    "numeric_code": "144",
    "official_name": "Democratic Socialist Republic of Sri Lanka",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LR",
    "country_name": "Liberia",
    "alpha3_code": "LBR",
    "numeric_code": "430",
    "official_name": "Republic of Liberia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LS",
    "country_name": "Lesotho",
    "alpha3_code": "LSO",
    "numeric_code": "426",
    "official_name": "Kingdom of Lesotho",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LT",
    "country_name": "Lithuania",
    "alpha3_code": "LTU",
    "numeric_code": "440",
    "official_name": "Republic of Lithuania",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LU",
    "country_name": "Luxembourg",
    "alpha3_code": "LUX",
    "numeric_code": "442",
    "official_name": "Grand Duchy of Luxembourg",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LV",
    "country_name": "Latvia",
    "alpha3_code": "LVA",
    "numeric_code": "428",
    "official_name": "Republic of Latvia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "LY",
    "country_name": "Libya",
    "alpha3_code": "LBY",
    "numeric_code": "434",
    "official_name": "Libya",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MA",
    "country_name": "Morocco",
    "alpha3_code": "MAR",
    "numeric_code": "504",
    "official_name": "Kingdom of Morocco",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MC",
    "country_name": "Monaco",
    "alpha3_code": "MCO",
    "numeric_code": "492",
    "official_name": "Principality of Monaco",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MD",
    "country_name": "Moldova, Republic of",
    "alpha3_code": "MDA",
    "numeric_code": "498",
    "official_name": "Republic of Moldova",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ME",
    "country_name": "Montenegro",
    "alpha3_code": "MNE",
    "numeric_code": "499",
    "official_name": "Montenegro",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MF",
    "country_name": "Saint Martin (French part)",
    "alpha3_code": "MAF",
    "numeric_code": "663",
    "official_name": "Saint Martin (French part)",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MG",
    "country_name": "Madagascar",
    "alpha3_code": "MDG",
    "numeric_code": "450",
    "official_name": "Republic of Madagascar",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MH",
    "country_name": "Marshall Islands",
    "alpha3_code": "MHL",
    "numeric_code": "584",
    "official_name": "Republic of the Marshall Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MK",
    "country_name": "North Macedonia",
    "alpha3_code": "MKD",
    "numeric_code": "807",
    "official_name": "Republic of North Macedonia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ML",
    "country_name": "Mali",
    "alpha3_code": "MLI",
    "numeric_code": "466",
    "official_name": "Republic of Mali",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MM",
    "country_name": "Myanmar",
    "alpha3_code": "MMR",
    "numeric_code": "104",
    "official_name": "Republic of Myanmar",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MN",
    "country_name": "Mongolia",
    "alpha3_code": "MNG",
    "numeric_code": "496",
    "official_name": "Mongolia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MO",
    "country_name": "Macao",
    "alpha3_code": "MAC",
    "numeric_code": "446",
    "official_name": "Macao Special Administrative Region of China",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MP",
    "country_name": "Northern Mariana Islands",
    "alpha3_code": "MNP",
    "numeric_code": "580",
    "official_name": "Commonwealth of the Northern Mariana Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MQ",
    "country_name": "Martinique",
    "alpha3_code": "MTQ",
    "numeric_code": "474",
    "official_name": "Martinique",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MR",
    "country_name": "Mauritania",
    "alpha3_code": "MRT",
    "numeric_code": "478",
    "official_name": "Islamic Republic of Mauritania",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MS",
    "country_name": "Montserrat",
    "alpha3_code": "MSR",
    "numeric_code": "500",
    "official_name": "Montserrat",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MT",
    "country_name": "Malta",
    "alpha3_code": "MLT",
    "numeric_code": "470",
    "official_name": "Republic of Malta",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MU",
    "country_name": "Mauritius",
    "alpha3_code": "MUS",
    "numeric_code": "480",
    "official_name": "Republic of Mauritius",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MV",
    "country_name": "Maldives",
    "alpha3_code": "MDV",
    "numeric_code": "462",
    "official_name": "Republic of Maldives",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MW",
    "country_name": "Malawi",
    "alpha3_code": "MWI",
    "numeric_code": "454",
    "official_name": "Republic of Malawi",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MX",
    "country_name": "Mexico",
    "alpha3_code": "MEX",
    "numeric_code": "484",
    "official_name": "United Mexican States",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MY",
    "country_name": "Malaysia",
    "alpha3_code": "MYS",
    "numeric_code": "458",
    "official_name": "Malaysia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "MZ",
    "country_name": "Mozambique",
    "alpha3_code": "MOZ",
    "numeric_code": "508",
    "official_name": "Republic of Mozambique",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NA",
    "country_name": "Namibia",
    "alpha3_code": "NAM",
    "numeric_code": "516",
    "official_name": "Republic of Namibia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NC",
    "country_name": "New Caledonia",
    "alpha3_code": "NCL",
    "numeric_code": "540",
    "official_name": "New Caledonia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NE",
    "country_name": "Niger",
    "alpha3_code": "NER",
    "numeric_code": "562",
    "official_name": "Republic of the Niger",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NF",
    "country_name": "Norfolk Island",
    "alpha3_code": "NFK",
    "numeric_code": "574",
    "official_name": "Norfolk Island",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NG",
    "country_name": "Nigeria",
    "alpha3_code": "NGA",
    "numeric_code": "566",
    "official_name": "Federal Republic of Nigeria",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NI",
    "country_name": "Nicaragua",
    "alpha3_code": "NIC",
    "numeric_code": "558",
    "official_name": "Republic of Nicaragua",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NL",
    "country_name": "Netherlands",
    "alpha3_code": "NLD",
    "numeric_code": "528",
    "official_name": "Kingdom of the Netherlands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NO",
    "country_name": "Norway",
    "alpha3_code": "NOR",
    "numeric_code": "578",
    "official_name": "Kingdom of Norway",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NP",
    "country_name": "Nepal",
    "alpha3_code": "NPL",
    "numeric_code": "524",
    "official_name": "Federal Democratic Republic of Nepal",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NR",
    "country_name": "Nauru",
    "alpha3_code": "NRU",
    "numeric_code": "520",
    "official_name": "Republic of Nauru",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NU",
    "country_name": "Niue",
    "alpha3_code": "NIU",
    "numeric_code": "570",
    "official_name": "Niue",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "NZ",
    "country_name": "New Zealand",
    "alpha3_code": "NZL",
    "numeric_code": "554",
    "official_name": "New Zealand",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "OM",
    "country_name": "Oman",
    "alpha3_code": "OMN",
    "numeric_code": "512",
    "official_name": "Sultanate of Oman",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PA",
    "country_name": "Panama",
    "alpha3_code": "PAN",
    "numeric_code": "591",
    "official_name": "Republic of Panama",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PE",
    "country_name": "Peru",
    "alpha3_code": "PER",
    "numeric_code": "604",
    "official_name": "Republic of Peru",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PF",
    "country_name": "French Polynesia",
    "alpha3_code": "PYF",
    "numeric_code": "258",
    "official_name": "French Polynesia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PG",
    "country_name": "Papua New Guinea",
    "alpha3_code": "PNG",
    "numeric_code": "598",
    "official_name": "Independent State of Papua New Guinea",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PH",
    "country_name": "Philippines",
    "alpha3_code": "PHL",
    "numeric_code": "608",
    "official_name": "Republic of the Philippines",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PK",
    "country_name": "Pakistan",
    "alpha3_code": "PAK",
    "numeric_code": "586",
    "official_name": "Islamic Republic of Pakistan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PL",
    "country_name": "Poland",
    "alpha3_code": "POL",
    "numeric_code": "616",
    "official_name": "Republic of Poland",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PM",
    "country_name": "Saint Pierre and Miquelon",
    "alpha3_code": "SPM",
    "numeric_code": "666",
    "official_name": "Saint Pierre and Miquelon",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PN",
    "country_name": "Pitcairn",
    "alpha3_code": "PCN",
    "numeric_code": "612",
    "official_name": "Pitcairn",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PR",
    "country_name": "Puerto Rico",
    "alpha3_code": "PRI",
    "numeric_code": "630",
    "official_name": "Puerto Rico",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PS",
    "country_name": "Palestine, State of",
    "alpha3_code": "PSE",
    "numeric_code": "275",
    "official_name": "the State of Palestine",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PT",
    "country_name": "Portugal",
    "alpha3_code": "PRT",
    "numeric_code": "620",
    "official_name": "Portuguese Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PW",
    "country_name": "Palau",
    "alpha3_code": "PLW",
    "numeric_code": "585",
    "official_name": "Republic of Palau",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "PY",
    "country_name": "Paraguay",
    "alpha3_code": "PRY",
    "numeric_code": "600",
    "official_name": "Republic of Paraguay",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "QA",
    "country_name": "Qatar",
    "alpha3_code": "QAT",
    "numeric_code": "634",
    "official_name": "State of Qatar",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "RE",
    "country_name": "Réunion",
    "alpha3_code": "REU",
    "numeric_code": "638",
    "official_name": "Réunion",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "RO",
    "country_name": "Romania",
    "alpha3_code": "ROU",
    "numeric_code": "642",
    "official_name": "Romania",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "RS",
    "country_name": "Serbia",
    "alpha3_code": "SRB",
    "numeric_code": "688",
    "official_name": "Republic of Serbia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "RU",
    "country_name": "Russian Federation",
    "alpha3_code": "RUS",
    "numeric_code": "643",
    "official_name": "Russian Federation",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "RW",
    "country_name": "Rwanda",
    "alpha3_code": "RWA",
    "numeric_code": "646",
    "official_name": "Rwandese Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SA",
    "country_name": "Saudi Arabia",
    "alpha3_code": "SAU",
    "numeric_code": "682",
    "official_name": "Kingdom of Saudi Arabia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SB",
    "country_name": "Solomon Islands",
    "alpha3_code": "SLB",
    "numeric_code": "090",
    "official_name": "Solomon Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SC",
    "country_name": "Seychelles",
    "alpha3_code": "SYC",
    "numeric_code": "690",
    "official_name": "Republic of Seychelles",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SD",
    "country_name": "Sudan",
    "alpha3_code": "SDN",
    "numeric_code": "729",
    "official_name": "Republic of the Sudan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SE",
    "country_name": "Sweden",
    "alpha3_code": "SWE",
    "numeric_code": "752",
    "official_name": "Kingdom of Sweden",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SG",
    "country_name": "Singapore",
    "alpha3_code": "SGP",
    "numeric_code": "702",
    "official_name": "Republic of Singapore",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SH",
    "country_name": "Saint Helena, Ascension and Tristan da Cunha",
    "alpha3_code": "SHN",
    "numeric_code": "654",
    "official_name": "Saint Helena, Ascension and Tristan da Cunha",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SI",
    "country_name": "Slovenia",
    "alpha3_code": "SVN",
    "numeric_code": "705",
    "official_name": "Republic of Slovenia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SJ",
    "country_name": "Svalbard and Jan Mayen",
    "alpha3_code": "SJM",
    "numeric_code": "744",
    "official_name": "Svalbard and Jan Mayen",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SK",
    "country_name": "Slovakia",
    "alpha3_code": "SVK",
    "numeric_code": "703",
    "official_name": "Slovak Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SL",
    "country_name": "Sierra Leone",
    "alpha3_code": "SLE",
    "numeric_code": "694",
    "official_name": "Republic of Sierra Leone",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SM",
    "country_name": "San Marino",
    "alpha3_code": "SMR",
    "numeric_code": "674",
    "official_name": "Republic of San Marino",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SN",
    "country_name": "Senegal",
    "alpha3_code": "SEN",
    "numeric_code": "686",
    "official_name": "Republic of Senegal",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SO",
    "country_name": "Somalia",
    "alpha3_code": "SOM",
    "numeric_code": "706",
    "official_name": "Federal Republic of Somalia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SR",
    "country_name": "Suriname",
    "alpha3_code": "SUR",
    "numeric_code": "740",
    "official_name": "Republic of Suriname",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SS",
    "country_name": "South Sudan",
    "alpha3_code": "SSD",
    "numeric_code": "728",
    "official_name": "Republic of South Sudan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ST",
    "country_name": "Sao Tome and Principe",
    "alpha3_code": "STP",
    "numeric_code": "678",
    "official_name": "Democratic Republic of Sao Tome and Principe",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SV",
    "country_name": "El Salvador",
    "alpha3_code": "SLV",
    "numeric_code": "222",
    "official_name": "Republic of El Salvador",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SX",
    "country_name": "Sint Maarten (Dutch part)",
    "alpha3_code": "SXM",
    "numeric_code": "534",
    "official_name": "Sint Maarten (Dutch part)",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SY",
    "country_name": "Syrian Arab Republic",
    "alpha3_code": "SYR",
    "numeric_code": "760",
    "official_name": "Syrian Arab Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "SZ",
    "country_name": "Eswatini",
    "alpha3_code": "SWZ",
    "numeric_code": "748",
    "official_name": "Kingdom of Eswatini",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TC",
    "country_name": "Turks and Caicos Islands",
    "alpha3_code": "TCA",
    "numeric_code": "796",
    "official_name": "Turks and Caicos Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TD",
    "country_name": "Chad",
    "alpha3_code": "TCD",
    "numeric_code": "148",
    "official_name": "Republic of Chad",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TF",
    "country_name": "French Southern Territories",
    "alpha3_code": "ATF",
    "numeric_code": "260",
    "official_name": "French Southern Territories",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TG",
    "country_name": "Togo",
    "alpha3_code": "TGO",
    "numeric_code": "768",
    "official_name": "Togolese Republic",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TH",
    "country_name": "Thailand",
    "alpha3_code": "THA",
    "numeric_code": "764",
    "official_name": "Kingdom of Thailand",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TJ",
    "country_name": "Tajikistan",
    "alpha3_code": "TJK",
    "numeric_code": "762",
    "official_name": "Republic of Tajikistan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TK",
    "country_name": "Tokelau",
    "alpha3_code": "TKL",
    "numeric_code": "772",
    "official_name": "Tokelau",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TL",
    "country_name": "Timor-Leste",
    "alpha3_code": "TLS",
    "numeric_code": "626",
    "official_name": "Democratic Republic of Timor-Leste",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TM",
    "country_name": "Turkmenistan",
    "alpha3_code": "TKM",
    "numeric_code": "795",
    "official_name": "Turkmenistan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TN",
    "country_name": "Tunisia",
    "alpha3_code": "TUN",
    "numeric_code": "788",
    "official_name": "Republic of Tunisia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TO",
    "country_name": "Tonga",
    "alpha3_code": "TON",
    "numeric_code": "776",
    "official_name": "Kingdom of Tonga",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TR",
    "country_name": "Türkiye",
    "alpha3_code": "TUR",
    "numeric_code": "792",
    "official_name": "Republic of Türkiye",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TT",
    "country_name": "Trinidad and Tobago",
    "alpha3_code": "TTO",
    "numeric_code": "780",
    "official_name": "Republic of Trinidad and Tobago",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TV",
    "country_name": "Tuvalu",
    "alpha3_code": "TUV",
    "numeric_code": "798",
    "official_name": "Tuvalu",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TW",
    "country_name": "Taiwan, Province of China",
    "alpha3_code": "TWN",
    "numeric_code": "158",
    "official_name": "Taiwan, Province of China",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "TZ",
    "country_name": "Tanzania, United Republic of",
    "alpha3_code": "TZA",
    "numeric_code": "834",
    "official_name": "United Republic of Tanzania",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "UA",
    "country_name": "Ukraine",
    "alpha3_code": "UKR",
    "numeric_code": "804",
    "official_name": "Ukraine",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "UG",
    "country_name": "Uganda",
    "alpha3_code": "UGA",
    "numeric_code": "800",
    "official_name": "Republic of Uganda",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "UM",
    "country_name": "United States Minor Outlying Islands",
    "alpha3_code": "UMI",
    "numeric_code": "581",
    "official_name": "United States Minor Outlying Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "US",
    "country_name": "United States",
    "alpha3_code": "USA",
    "numeric_code": "840",
    "official_name": "United States of America",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "UY",
    "country_name": "Uruguay",
    "alpha3_code": "URY",
    "numeric_code": "858",
    "official_name": "Eastern Republic of Uruguay",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "UZ",
    "country_name": "Uzbekistan",
    "alpha3_code": "UZB",
    "numeric_code": "860",
    "official_name": "Republic of Uzbekistan",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "VA",
    "country_name": "Holy See (Vatican City State)",
    "alpha3_code": "VAT",
    "numeric_code": "336",
    "official_name": "Holy See (Vatican City State)",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "VC",
    "country_name": "Saint Vincent and the Grenadines",
    "alpha3_code": "VCT",
    "numeric_code": "670",
    "official_name": "Saint Vincent and the Grenadines",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "VE",
    "country_name": "Venezuela, Bolivarian Republic of",
    "alpha3_code": "VEN",
    "numeric_code": "862",
    "official_name": "Bolivarian Republic of Venezuela",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "VG",
    "country_name": "Virgin Islands, British",
    "alpha3_code": "VGB",
    "numeric_code": "092",
    "official_name": "British Virgin Islands",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "VI",
    "country_name": "Virgin Islands, U.S.",
    "alpha3_code": "VIR",
    "numeric_code": "850",
    "official_name": "Virgin Islands of the United States",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "VN",
    "country_name": "Viet Nam",
    "alpha3_code": "VNM",
    "numeric_code": "704",
    "official_name": "Socialist Republic of Viet Nam",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "VU",
    "country_name": "Vanuatu",
    "alpha3_code": "VUT",
    "numeric_code": "548",
    "official_name": "Republic of Vanuatu",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "WF",
    "country_name": "Wallis and Futuna",
    "alpha3_code": "WLF",
    "numeric_code": "876",
    "official_name": "Wallis and Futuna",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "WS",
    "country_name": "Samoa",
    "alpha3_code": "WSM",
    "numeric_code": "882",
    "official_name": "Independent State of Samoa",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "YE",
    "country_name": "Yemen",
    "alpha3_code": "YEM",
    "numeric_code": "887",
    "official_name": "Republic of Yemen",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "YT",
    "country_name": "Mayotte",
    "alpha3_code": "MYT",
    "numeric_code": "175",
    "official_name": "Mayotte",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ZA",
    "country_name": "South Africa",
    "alpha3_code": "ZAF",
    "numeric_code": "710",
    "official_name": "Republic of South Africa",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ZM",
    "country_name": "Zambia",
    "alpha3_code": "ZMB",
    "numeric_code": "894",
    "official_name": "Republic of Zambia",
    "region": "",
    "sub_region": ""
  },
  {
    "country_code": "ZW",
    "country_name": "Zimbabwe",
    "alpha3_code": "ZWE",
    "numeric_code": "716",
    "official_name": "Republic of Zimbabwe",
    "region": "",
    "sub_region": ""
  }
]

SILVER_EXTRA_COLUMNS = [
    {"name": "country_name", "type": "string"},
    {"name": "country_alpha3_code", "type": "string"},
    {"name": "country_region", "type": "string"},
    {"name": "country_sub_region", "type": "string"},
]


def client() -> yt.YtClient:
    return yt.YtClient(proxy=YT_PROXY, token=YT_TOKEN)


def create_or_replace_iso_dim(c: yt.YtClient) -> None:
    parent = REF_PATH.rsplit("/", 1)[0]
    if parent:
        c.create("map_node", parent, recursive=True, ignore_existing=True)

    if c.exists(REF_PATH):
        c.remove(REF_PATH, force=True)

    c.create("table", REF_PATH, recursive=True, attributes={"schema": ISO_SCHEMA})
    c.write_table(f"<append=%false>{REF_PATH}", ISO_ROWS, format="json")
    print(f"[OK] recreated {REF_PATH} with {len(ISO_ROWS)} row(s)")


def ensure_silver_schema_columns(c: yt.YtClient) -> None:
    if not c.exists(SILVER_PATH):
        raise RuntimeError(f"Silver table does not exist: {SILVER_PATH}")

    schema = list(c.get(f"{SILVER_PATH}/@schema"))
    existing = {col["name"] for col in schema}

    to_add = [col for col in SILVER_EXTRA_COLUMNS if col["name"] not in existing]
    if not to_add:
        print(f"[OK] silver schema already has ISO enrichment columns: {SILVER_PATH}")
        return

    new_schema = schema[:]
    insert_after = "country_code"
    insert_idx = next((i for i, col in enumerate(new_schema) if col["name"] == insert_after), None)

    if insert_idx is None:
        new_schema.extend(to_add)
    else:
        for col in reversed(to_add):
            new_schema.insert(insert_idx + 1, col)

    c.alter_table(SILVER_PATH, schema=new_schema)
    print(f"[OK] altered silver schema, added: {', '.join(col['name'] for col in to_add)}")


def main() -> int:
    c = client()
    create_or_replace_iso_dim(c)
    ensure_silver_schema_columns(c)
    print("[DONE] full ISO dim setup and silver schema update complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
