Parcel Extractor and Ownership Data Parser For El Dorado County, CA
===================================================================

This script was set up to use the El Dorado County Feature Extraction geoprocessing service. A web app of this can be found [here](https://see-eldorado.edcgov.us/ugotnetextracts/). This script bypasses the web app and goes directly to the [GP service](https://see-eldorado.edcgov.us/arcgis/rest/services/uGOTNETandEXTRACTS/geoservices/GPServer/Extract%20Data%20Task). 

## Script workflow

1. Query El Dorado Parcel REST endpoint to get latest date in `POLY_CREATE_DATE` field.
2. Compare that to the latest date in the local `SDE` or `FGDB` FC:

   If both dates are the same, script ends.
   
   If El Dorado County's date is newer, script continues to run.

3. Aquires `FGDB` of all parcels in the county, converts to `in_memory` layer, and adds ownership fields.
4. Regex comprehension begins and takes ownership information and seperates it our into `owner`, `address`, `city`, `state`, `zip`, and `country` fields.
5. Truncates the `SDE` or `FGSB` FC, then appends new data.
5. Profit!
