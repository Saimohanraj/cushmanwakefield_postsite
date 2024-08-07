import re
import os
import json
import scrapy
import hashlib
from datetime import datetime
from cushmanwakefield.items import CusmanwakefieldItem


class CusmanwakefieldsaleSpider(scrapy.Spider):
    
    name = "sale_scraper"
    
    no_record_count = 0

    current_date = datetime.now().strftime("%Y-%m-%d")
    
    custom_settings = {
        'ITEM_PIPELINES': {"cushmanwakefield.pipelines.CusmanwakefieldScraperPipeline": 300,},
        'FEED_EXPORT_ENCODING' : "utf-8",
        # 'FEEDS': {f"s3://{os.getenv('OUTPUT_BUCKET')}/output/daily_collections/cushmanwakefield/{current_date}/{name}.json": {"format": "json"}},
    }
    
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }
    
    def regex(self, word):
        word = re.sub(r'\s+', ' ', word)
        word = word.replace('\n', ' ')

        return word
    
    def start_requests(self):
        self.crawler.stats.inc_value('no_record_count')
        url = "https://www.cushmanwakefield.com/coveo/rest/search/v2?sitecoreItemUri=sitecore%3A%2F%2Fweb%2F%7B386BA9C4-4FFD-4114-BC84-19B8B4FCA48F%7D%3Flang%3Den%26amp%3Bver%3D4&siteName=CushmanWakefield"
        payload = "actionsHistory=%5B%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T13%3A52%3A07.484Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T12%3A52%3A48.919Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T12%3A00%3A46.304Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T11%3A55%3A56.594Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T11%3A10%3A13.831Z%5C%22%22%7D%5D&referrer=&analytics=%7B%22clientId%22%3A%22d382ebeb-9d94-4f9f-d02e-b30ea4ba8ef6%22%2C%22documentLocation%22%3A%22https%3A%2F%2Fwww.cushmanwakefield.com%2Fen%2Funited-states%2Fproperties%2Finvest%2Finvest-property-search%23sort%3D%2540propertylastupdateddate%2520descending%22%2C%22documentReferrer%22%3A%22%22%2C%22pageId%22%3A%22%22%2C%22actionCause%22%3A%22interfaceLoad%22%2C%22customData%22%3A%7B%22JSUIVersion%22%3A%222.10110.0%3B2.10110.0%22%2C%22pageFullPath%22%3A%22%2Fsitecore%2Fcontent%2FWebsites%2Fcushwake%2Fhome%2Famericas%2FUnited%20States%2Fproperties%2FInvest%2FInvest%20Property%20Search%22%2C%22sitename%22%3A%22CushmanWakefield%22%2C%22siteName%22%3A%22CushmanWakefield%22%2C%22context_device%22%3A%22Default%22%2C%22context_isAnonymous%22%3A%22true%22%2C%22context_country%22%3A%22SG%22%7D%2C%22originContext%22%3A%22WebsiteSearch%22%7D&visitorId=d382ebeb-9d94-4f9f-d02e-b30ea4ba8ef6&isGuestUser=false&aq=(NOT%20%40z95xtemplate%3D%3D(ADB6CA4F03EF4F47B9AC9CE2BA53FF97%2CFE5DD82648C6436DB87A7C4210C7413B))%20(((%40z95xtemplate%3D%3D17750C6E93C147B282BE8C225D45A6E0%20%40propertylistingtype%3D%3DBuy)%20NOT%20%40z95xtemplate%3D%3D(ADB6CA4F03EF4F47B9AC9CE2BA53FF97%2CFE5DD82648C6436DB87A7C4210C7413B)))%20((%40z95xpath%3D%3D9720B708FE0C46A289BDB4B798A85300%20NOT%20%40z95xtemplate%3D%3D(ADB6CA4F03EF4F47B9AC9CE2BA53FF97%2CFE5DD82648C6436DB87A7C4210C7413B)))&cq=(%40z95xlanguage%3D%3Den)%20(%40z95xlatestversion%3D%3D1)%20(%40source%3D%3D%22Coveo_cw-prod-amrgws-cd-web_index%20-%20PRODUCTION%22)&searchHub=Invest%20Property%20Search&locale=en&pipeline=Properties&maximumAge=900000&firstResult=0&numberOfResults=12&excerptLength=200&enableDidYouMean=false&sortCriteria=%40propertylastupdateddate%20descending&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40propertytype%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertycountry%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertystateorprovince%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22alphaascending%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertycity%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22alphaascending%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertyavailablespaceunit%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertybrokernames%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22alphaascending%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertyavailablespacevalue%22%2C%22maximumNumberOfValues%22%3A15%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22rangeValues%22%3A%5B%7B%22start%22%3A%221%22%2C%22end%22%3A%22500%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%221%20-%20500%22%7D%2C%7B%22start%22%3A%22500%22%2C%22end%22%3A%221000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22500%20-%201%2C000%22%7D%2C%7B%22start%22%3A%221000%22%2C%22end%22%3A%222500%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%221%2C000%20-%202%2C500%22%7D%2C%7B%22start%22%3A%222500%22%2C%22end%22%3A%225000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%222%2C500%20-%205%2C000%22%7D%2C%7B%22start%22%3A%225000%22%2C%22end%22%3A%2210000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%225%2C000%20-%2010%2C000%22%7D%2C%7B%22start%22%3A%2210000%22%2C%22end%22%3A%2220000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%2210%2C000%20-%2020%2C000%22%7D%2C%7B%22start%22%3A%2220000%22%2C%22end%22%3A%2250000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%2220%2C000%20-%2050%2C000%22%7D%2C%7B%22start%22%3A%2250000%22%2C%22end%22%3A%22100000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%2250%2C000%20%E2%80%93%20100%2C000%20%22%7D%2C%7B%22start%22%3A%22100000%22%2C%22end%22%3A%22200000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22100%2C000%20%E2%80%93%20200%2C000%20%22%7D%2C%7B%22start%22%3A%22200000%22%2C%22end%22%3A%22300000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22200%2C000%20%E2%80%93%20300%2C000%20%22%7D%2C%7B%22start%22%3A%22300000%22%2C%22end%22%3A%22400000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22300%2C000%20%E2%80%93%20400%2C000%20%22%7D%2C%7B%22start%22%3A%22400000%22%2C%22end%22%3A%22500000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22400%2C000%20%E2%80%93%20500%2C000%20%22%7D%2C%7B%22start%22%3A%22500000%22%2C%22end%22%3A%22750000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22500%2C000%20%E2%80%93%20750%2C000%20%22%7D%2C%7B%22start%22%3A%22750000%22%2C%22end%22%3A%221000000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22750%2C000%20%E2%80%93%201%2C000%2C000%20%22%7D%2C%7B%22start%22%3A%221000000%22%2C%22end%22%3A%22100000000%22%2C%22endInclusive%22%3Atrue%2C%22label%22%3A%221%2C000%2C000%2B%22%7D%5D%7D%2C%7B%22field%22%3A%22%40propertypricevalue%22%2C%22maximumNumberOfValues%22%3A5%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22rangeValues%22%3A%5B%7B%22start%22%3A%220.1%22%2C%22end%22%3A%22500000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22%3C%20500000%22%7D%2C%7B%22start%22%3A%22500000%22%2C%22end%22%3A%221000000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22500000%20-%201000000%22%7D%2C%7B%22start%22%3A%221000000%22%2C%22end%22%3A%222500000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%221000000%20-%202500000%22%7D%2C%7B%22start%22%3A%222500000%22%2C%22end%22%3A%225000000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%222500000%20-%205000000%22%7D%2C%7B%22start%22%3A%225000000%22%2C%22end%22%3A%221000000000%22%2C%22endInclusive%22%3Atrue%2C%22label%22%3A%225000000%2B%22%7D%5D%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=Asia%2FCalcutta&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%22device%22%3A%22Default%22%2C%22isAnonymous%22%3A%22true%22%2C%22country%22%3A%22SG%22%7D&allowQueriesWithoutKeywords=true"
        yield scrapy.Request(url,method="POST",headers=self.headers,callback=self.parse,body=payload,dont_filter=True)
    
    def parse(self,response):
        json_response = json.loads(response.text)
        total_count = json_response['totalCount']
        for i in range(0,int(total_count)+12,12): 
            url = "https://www.cushmanwakefield.com/coveo/rest/search/v2?sitecoreItemUri=sitecore%3A%2F%2Fweb%2F%7B386BA9C4-4FFD-4114-BC84-19B8B4FCA48F%7D%3Flang%3Den%26amp%3Bver%3D4&siteName=CushmanWakefield"
            payload = f"actionsHistory=%5B%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T13%3A52%3A07.484Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T12%3A52%3A48.919Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T12%3A00%3A46.304Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T11%3A55%3A56.594Z%5C%22%22%7D%2C%7B%22name%22%3A%22Query%22%2C%22time%22%3A%22%5C%222024-06-12T11%3A10%3A13.831Z%5C%22%22%7D%5D&referrer=&analytics=%7B%22clientId%22%3A%22d382ebeb-9d94-4f9f-d02e-b30ea4ba8ef6%22%2C%22documentLocation%22%3A%22https%3A%2F%2Fwww.cushmanwakefield.com%2Fen%2Funited-states%2Fproperties%2Finvest%2Finvest-property-search%23sort%3D%2540propertylastupdateddate%2520descending%22%2C%22documentReferrer%22%3A%22%22%2C%22pageId%22%3A%22%22%2C%22actionCause%22%3A%22interfaceLoad%22%2C%22customData%22%3A%7B%22JSUIVersion%22%3A%222.10110.0%3B2.10110.0%22%2C%22pageFullPath%22%3A%22%2Fsitecore%2Fcontent%2FWebsites%2Fcushwake%2Fhome%2Famericas%2FUnited%20States%2Fproperties%2FInvest%2FInvest%20Property%20Search%22%2C%22sitename%22%3A%22CushmanWakefield%22%2C%22siteName%22%3A%22CushmanWakefield%22%2C%22context_device%22%3A%22Default%22%2C%22context_isAnonymous%22%3A%22true%22%2C%22context_country%22%3A%22SG%22%7D%2C%22originContext%22%3A%22WebsiteSearch%22%7D&visitorId=d382ebeb-9d94-4f9f-d02e-b30ea4ba8ef6&isGuestUser=false&aq=(NOT%20%40z95xtemplate%3D%3D(ADB6CA4F03EF4F47B9AC9CE2BA53FF97%2CFE5DD82648C6436DB87A7C4210C7413B))%20(((%40z95xtemplate%3D%3D17750C6E93C147B282BE8C225D45A6E0%20%40propertylistingtype%3D%3DBuy)%20NOT%20%40z95xtemplate%3D%3D(ADB6CA4F03EF4F47B9AC9CE2BA53FF97%2CFE5DD82648C6436DB87A7C4210C7413B)))%20((%40z95xpath%3D%3D9720B708FE0C46A289BDB4B798A85300%20NOT%20%40z95xtemplate%3D%3D(ADB6CA4F03EF4F47B9AC9CE2BA53FF97%2CFE5DD82648C6436DB87A7C4210C7413B)))&cq=(%40z95xlanguage%3D%3Den)%20(%40z95xlatestversion%3D%3D1)%20(%40source%3D%3D%22Coveo_cw-prod-amrgws-cd-web_index%20-%20PRODUCTION%22)&searchHub=Invest%20Property%20Search&locale=en&pipeline=Properties&maximumAge=900000&firstResult={i}&numberOfResults=12&excerptLength=200&enableDidYouMean=false&sortCriteria=%40propertylastupdateddate%20descending&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&groupBy=%5B%7B%22field%22%3A%22%40propertytype%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertycountry%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertystateorprovince%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22alphaascending%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertycity%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22alphaascending%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertyavailablespaceunit%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22occurrences%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertybrokernames%22%2C%22maximumNumberOfValues%22%3A6%2C%22sortCriteria%22%3A%22alphaascending%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22allowedValues%22%3A%5B%5D%7D%2C%7B%22field%22%3A%22%40propertyavailablespacevalue%22%2C%22maximumNumberOfValues%22%3A15%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22rangeValues%22%3A%5B%7B%22start%22%3A%221%22%2C%22end%22%3A%22500%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%221%20-%20500%22%7D%2C%7B%22start%22%3A%22500%22%2C%22end%22%3A%221000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22500%20-%201%2C000%22%7D%2C%7B%22start%22%3A%221000%22%2C%22end%22%3A%222500%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%221%2C000%20-%202%2C500%22%7D%2C%7B%22start%22%3A%222500%22%2C%22end%22%3A%225000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%222%2C500%20-%205%2C000%22%7D%2C%7B%22start%22%3A%225000%22%2C%22end%22%3A%2210000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%225%2C000%20-%2010%2C000%22%7D%2C%7B%22start%22%3A%2210000%22%2C%22end%22%3A%2220000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%2210%2C000%20-%2020%2C000%22%7D%2C%7B%22start%22%3A%2220000%22%2C%22end%22%3A%2250000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%2220%2C000%20-%2050%2C000%22%7D%2C%7B%22start%22%3A%2250000%22%2C%22end%22%3A%22100000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%2250%2C000%20%E2%80%93%20100%2C000%20%22%7D%2C%7B%22start%22%3A%22100000%22%2C%22end%22%3A%22200000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22100%2C000%20%E2%80%93%20200%2C000%20%22%7D%2C%7B%22start%22%3A%22200000%22%2C%22end%22%3A%22300000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22200%2C000%20%E2%80%93%20300%2C000%20%22%7D%2C%7B%22start%22%3A%22300000%22%2C%22end%22%3A%22400000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22300%2C000%20%E2%80%93%20400%2C000%20%22%7D%2C%7B%22start%22%3A%22400000%22%2C%22end%22%3A%22500000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22400%2C000%20%E2%80%93%20500%2C000%20%22%7D%2C%7B%22start%22%3A%22500000%22%2C%22end%22%3A%22750000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22500%2C000%20%E2%80%93%20750%2C000%20%22%7D%2C%7B%22start%22%3A%22750000%22%2C%22end%22%3A%221000000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22750%2C000%20%E2%80%93%201%2C000%2C000%20%22%7D%2C%7B%22start%22%3A%221000000%22%2C%22end%22%3A%22100000000%22%2C%22endInclusive%22%3Atrue%2C%22label%22%3A%221%2C000%2C000%2B%22%7D%5D%7D%2C%7B%22field%22%3A%22%40propertypricevalue%22%2C%22maximumNumberOfValues%22%3A5%2C%22sortCriteria%22%3A%22nosort%22%2C%22injectionDepth%22%3A1000%2C%22completeFacetWithStandardValues%22%3Atrue%2C%22rangeValues%22%3A%5B%7B%22start%22%3A%220.1%22%2C%22end%22%3A%22500000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22%3C%20500000%22%7D%2C%7B%22start%22%3A%22500000%22%2C%22end%22%3A%221000000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%22500000%20-%201000000%22%7D%2C%7B%22start%22%3A%221000000%22%2C%22end%22%3A%222500000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%221000000%20-%202500000%22%7D%2C%7B%22start%22%3A%222500000%22%2C%22end%22%3A%225000000%22%2C%22endInclusive%22%3Afalse%2C%22label%22%3A%222500000%20-%205000000%22%7D%2C%7B%22start%22%3A%225000000%22%2C%22end%22%3A%221000000000%22%2C%22endInclusive%22%3Atrue%2C%22label%22%3A%225000000%2B%22%7D%5D%7D%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=Asia%2FCalcutta&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%22device%22%3A%22Default%22%2C%22isAnonymous%22%3A%22true%22%2C%22country%22%3A%22SG%22%7D&allowQueriesWithoutKeywords=true"
            yield scrapy.Request(url,method="POST",headers=self.headers,callback=self.parse_mparse,body=payload,dont_filter=True)
    
    def parse_mparse(self,response):
        url_collection = json.loads(response.text)
        for urls in url_collection['results']:
            url = urls['ClickUri'].replace('sitecore-','')
            yield scrapy.Request(url,headers=self.headers,callback=self.parse_detail)
        self.crawler.stats.set_value('no_record_count',self.no_record_count)
            
    async def parse_detail(self,response):
        if response.status==200:
            cusmanwakeItem = CusmanwakefieldItem()
            cusmanwakeItem['property_url'] = response.url
            cusmanwakeItem['title'] = response.xpath('//h1/text()').get('').strip()
            cusmanwakeItem['category'] = response.xpath('//p[@class="page-title-tags"]/text()').get('').strip()
            cusmanwakeItem['address'] = ' '.join([i.strip() for i in response.xpath('//h5[@class="page-title-sub"]/text()').getall()])
            cusmanwakeItem['building_class']=response.xpath('//dt[contains(text(),"Building Class:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['investment_detail']=response.xpath('//span[contains(@class,"investment_detail")]/text()').get('').strip()
            cusmanwakeItem['contract_detail']=response.xpath('//span[contains(@class,"inContract_detail")]/text()').get('').strip()
            cusmanwakeItem['available_space']=response.xpath('//dt[contains(text(),"Available Space:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['rental_price']=response.xpath('//dt[contains(text(),"Rental Price:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['building_size']=response.xpath('//dt[contains(text(),"Building Size:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['construction_status']=response.xpath('//dt[contains(text(),"Construction Status:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['sublease']=response.xpath('//dt[contains(text(),"Sublease:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['rate_type']=response.xpath('//dt[contains(text(),"Rate Type:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['lot_size']=response.xpath('//dt[contains(text(),"Lot Size:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['year_built']=response.xpath('//dt[contains(text(),"Year Built:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['max_contiguous']=response.xpath('//dt[contains(text(),"Max Contiguous:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['min_divisible']=response.xpath('//dt[contains(text(),"Min Divisible:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['sale_price']=response.xpath('//dt[contains(text(),"Sale Price:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['price_per_unit'] = response.xpath('//dt[contains(text(),"Price Per Unit:")]/following-sibling::dd[1]/text()').get('').strip()
            cusmanwakeItem['images'] = response.xpath('//div[contains(@class,"carousel-item")]/img/@src').getall()
            cusmanwakeItem['key_features'] = ' '.join([i.strip() for i in response.xpath('//h6[contains(text(),"Key Features")]/following-sibling::ul/li//text()').getall()]).strip()
            cusmanwakeItem['description'] = self.regex(' '.join([i.strip() for i in response.xpath('//div[@class="page-content-body rich-text"]//text()').getall()]).strip())
            cusmanwakeItem['property_brochure'] = response.xpath('//a[contains(text()," Property Brochure")]/@href').getall()       
            broker_multi = []
            for main_broker_vcard in response.xpath('//a[contains(text()," Download VCard")]'):
                broker_dict = {}
                broker_dict['broker_vcard'] = ''
                broker_dict['phone_number'] =''
                broker_dict['broker_email'] =''
                broker_dict['broker_location'] = ''
                broker_dict['broker_designation']  = ''
                broker_dict['broker_name'] = ''
                broker_dict['broker_profile_url'] = ''   
                main_broker = main_broker_vcard.xpath('./@href').get('').strip()
                if main_broker.startswith('/'):
                    broker_dict['broker_vcard'] = response.urljoin(main_broker)                        
                    response_api = await self.variant_process(broker_dict['broker_vcard'])
                    broker_name = re.findall(r'FN\:([\W\w]*?)ADR',response_api.text)
                    if broker_name:
                        broker_dict['broker_name'] = self.regex(broker_name[0]).strip()
                    profile_url = re.findall(r'URL\;WORK\:([\W\w]*?)EMAIL',response_api.text)
                    if profile_url:
                        broker_dict['broker_profile_url'] = self.regex(profile_url[0]).strip()
                    broker_email= re.findall(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',response_api.text)
                    if broker_email:
                        broker_dict['broker_email'] = self.regex(broker_email[0]).strip()
                    phone_num = re.findall(r'TEL\;WORK\;VOICE\:([^>]*?)TEL',response_api.text)
                    if phone_num:
                        broker_dict['phone_number'] = self.regex(phone_num[0]).strip()
                    broker_designation = re.findall(r'TITLE\:([\W\w]*?)TEL',response_api.text)
                    if broker_designation:
                        broker_dict['broker_designation'] = self.regex(broker_designation[0]).strip()
                    broker_location = re.findall(r'ADR;WORK\:\;\;([\W\w]*?)ORG',response_api.text)
                    if broker_location:
                        broker_dict['broker_location'] = self.regex(broker_location[0]).strip()
                broker_multi.append(broker_dict) 
            cusmanwakeItem['broker'] =  broker_multi   
            cusmanwakeItem['scraped_date'] = datetime.now().strftime('%Y%m%d')
            all_values = [str(cusmanwakeItem[key]) for key in dict(cusmanwakeItem) if key not in ['images', 'scraped_date']]
            hash_obj = hashlib.md5(('_'.join(all_values)).encode('utf-8'))
            hash = hash_obj.hexdigest()
            cusmanwakeItem['hash'] = hash 
            yield cusmanwakeItem            
            
        else:
            self.log(f'Bad Response ---> {response.status}')
            self.no_record_count += 1
        
    async def variant_process(self, url):
        request = scrapy.Request(url,dont_filter=True)
        response = await self.crawler.engine.download(request)
        return response
