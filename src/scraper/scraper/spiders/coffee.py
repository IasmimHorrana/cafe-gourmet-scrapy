import scrapy


class CoffeeSpider(scrapy.Spider):
    name = "coffee"
    allowed_domains = ["lista.mercadolivre.com.br"]
    start_urls = ["https://lista.mercadolivre.com.br/cafe-gourmet?sb=all_mercadolibre#D[A:cafe%20gourmet]"]
    max_pages = 10
    items_per_page = 50

    def parse(self, response):
        products = response.css('div.ui-search-result__wrapper')

        for product in products:
            prices = product.css('span.andes-money-amount__fraction::text').getall()

            yield {
                'name': product.css('a.poly-component__title::text').get(),
                'old_price': prices[0] if len(prices) > 0 else None,
                'new_price': prices[1] if len(prices) > 1 else None,
                'promotion': product.css('span.andes-money-amount__discount::text').get(),
                'reviews_amount': product.css('span.poly-reviews__total::text').get(),
                'reviews_rating_number': product.css('span.poly-reviews__rating::text').get()
            }


        current_page = int(response.meta.get("page", 1))
        
        if current_page < self.max_pages:
            next_offset = (current_page * self.items_per_page) + 1 
            next_page = f"https://lista.mercadolivre.com.br/alimentos-bebidas/mercearia/infusoes/cafe/moido-grao/cafe-gourmet_Desde_{next_offset}_NoIndex_True?sb=all_mercadolibre"
            yield scrapy.Request(next_page, callback=self.parse, meta={"page": current_page + 1})

