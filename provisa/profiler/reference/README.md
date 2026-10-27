# Classification reference lists

The profiler's reference classification kinds (`provisa/profiler/classification_kinds.yaml`) test
a column's sampled values against these lists. Each file holds one normalised value per line:
trimmed, case-folded, with inner spaces collapsed.

| File | Holds | From |
|---|---|---|
| `countries.txt.gz` | Country names | GeoNames `countryInfo.txt`, Country column |
| `country_codes.txt.gz` | ISO 3166 alpha-2 and alpha-3 codes | GeoNames `countryInfo.txt` |
| `currency_codes.txt.gz` | ISO 4217 codes of the countries' currencies | GeoNames `countryInfo.txt` |
| `language_codes.txt.gz` | ISO 639-1 codes and both ISO 639-2 forms | GeoNames `iso-languagecodes.txt` |
| `cities.txt.gz` | Names of cities of 15,000 people or more | GeoNames `cities15000.txt`, name and asciiname |

`scripts/build_classification_reference.py` writes them from the GeoNames dump files. Do not edit
them by hand; run the script.

## Attribution

The lists are derived from the [GeoNames](https://www.geonames.org/) geographical database, used
under the [Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/).
See `THIRD_PARTY_NOTICES` in this directory.
