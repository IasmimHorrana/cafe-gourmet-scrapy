import psycopg2
from scrapy.exceptions import DropItem

class PostgresPipeline:
    # Chamado quando o spider é iniciado
    def open_spider(self, spider):
        # Conecta ao banco Postgres usando psycopg2
        self.conn = psycopg2.connect(
            dbname="cafe_db",
            user="cafe_user",
            password="cafe_pass",
            host="localhost",  # nome do serviço no docker-compose
            port=5433
        )
        # Cria um cursor, que é usado para executar comandos SQL
        self.cur = self.conn.cursor()

    # Chamado quando o spider termina de rodar
    def close_spider(self, spider):
        # Confirma todas as alterações pendentes
        self.conn.commit()
        # Fecha o cursor e a conexão com o banco
        self.cur.close()
        self.conn.close()

    # Chamado para cada item (cada café) que o Spider retorna
    def process_item(self, item, spider):
        try:
            # Comando SQL para inserir os dados do café na tabela raw.coffee_products
            self.cur.execute("""
                INSERT INTO raw.coffee_products 
                (name, old_price, new_price, promotion_label, promotion_value, reviews_amount, reviews_rating_number, scraped_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            """, (
                item["name"],                
                item["old_price"],           
                item["new_price"],           
                item.get("promotion_label"),
                item.get("promotion_value"),          
                item["reviews_amount"],      
                item["reviews_rating_number"], 
            ))
            # Confirma a inserção no banco
            self.conn.commit()
        except Exception as e:
            # Se ocorrer algum erro, registra no log do spider
            spider.logger.error(f"Erro ao inserir no Postgres: {e}")
            # Cancela a inserção do item problemático
            raise DropItem(f"Erro ao inserir no Postgres: {e}")
        # Retorna o item para que o pipeline continue (ou outros pipelines possam processar)
        return item
