# Deleted items at SDC (Wikimedia Commons)
A Wikidata bot that reports deleted Wikidata entites which are being used via SDC at Wikimedia Commons

In order to work, the bot needs a login cookie for the [Wikimedia Commons Query Service (WCQS)](https://commons-query.wikimedia.org/) as described on [Commons:SPARQL query service/API endpoint](https://commons.wikimedia.org/wiki/Commons:SPARQL_query_service/API_endpoint).

The Wikidata page edited by this bot is [Wikidata:Database reports/Deleted Wikidata entities used in SDC](https://www.wikidata.org/wiki/Wikidata:Database_reports/Deleted_Wikidata_entities_used_in_SDC).

## Technical requirements
The bot is currently scheduled to run weekly on [Toolforge](https://wikitech.wikimedia.org/wiki/Portal:Toolforge) from within the `msynbot` tool account. It depends on the [shared pywikibot files](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Pywikibot#Using_the_shared_Pywikibot_files_(recommended_setup)) and is running in a Kubernetes environment using Python 3.11.2.