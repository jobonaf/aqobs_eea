curl -X POST "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls" \
  -H "Content-Type: application/json" \
  -d '{
    "countries": ["IT"],
    "cities": [],
    "pollutants": ["PM10"],
    "dataset": 2,
    "email": "giovanni.bonafe@arpa.fvg.it"
  }'