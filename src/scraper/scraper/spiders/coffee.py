import scrapy


class CoffeeSpider(scrapy.Spider):
    name = "coffee"
    allowed_domains = ["lista.mercadolivre.com.br"]
    start_urls = ["https://lista.mercadolivre.com.br/cafe-gourmet?sb=all_mercadolibre#D[A:cafe%20gourmet]"]

    def parse(self, response):
        products = response.css('div.ui-search-result__wrapper')

        for product in products:

            prices = product.css('span.andes-money-amount__fraction::text').getall()

            yield {
                'name' : product.css('a.poly-component__title::text').get(),
                'old_price' : prices [0] if len(prices) > 0 else None,
                'new_price' :  prices [1] if len(prices) > 1 else None
                #'promotion' : andes-money-amount__discount
                #'reviews_amount' : 
                #'reviews_rating_number' : 
            }
        
        
        
        
        """
        next_page = response.css('[rel="next"]::attr(href)').get()  
        if next_page:
            yield response.follow(next_page, callback=self.parse)
        """
