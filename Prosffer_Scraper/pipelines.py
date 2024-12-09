# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import random
import string
import scrapy
import psycopg
from psycopg import sql
import environs
from pathlib import Path
from scrapy.exceptions import NotConfigured
from .mapping import CATEGORY_MAPPING


class ProsfferScraperPipeline:
    def generate_id_tag(self, store, name):
        # Generate unique id_tag using store and product name
        store_part = ''.join(store.split())[:3].lower()
        name_part = ''.join(name.split())[:3].lower()
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
        return f"{store_part}_{name_part}_{random_suffix}"

    def refine_category_with_context(self, product_name, original_category):
        """
        Refines the category based on product name keywords and original category.
        Prioritizes the most specific match.
        """
        keyword_priority = [
            ('milch', 'Dairy Products'),
            ('käse', 'Dairy Products'),
            ('butter', 'Dairy Products'),
            ('saft', 'Juices'),
            ('voelkel demeter', 'Juices'),
            ('smoothie', 'Juices'),
            ('whiskey', 'Spirits'),
            ('cola', 'Soft Drinks & Water'),
            ('wasser', 'Soft Drinks & Water'),
            ('tee', 'Tea & Coffee'),
            ('kaffee', 'Tea & Coffee'),
            ('cafe', 'Tea & Coffee'),
            ('caffee', 'Tea & Coffee'),
            ('caffè', 'Tea & Coffee'),
            ('espresso', 'Tea & Coffee'),
            ('entkoffeiniert', 'Tea & Coffee'),
            ('energy drink', 'Soft Drinks & Water'),
            # ('wein', 'Wine'),
            ('rosé', 'Wine'),
            ('bier', 'Beer'),
            ('vodka', 'Spirits'),
            ('Liqueur', 'Spirits'),
            ('tequila', 'Spirits'),
            ('brot', 'Bakery Products & Bread'),
            ('brezel', 'Bakery Products & Bread'),
            ('croissant', 'Bakery Products & Bread'),
            ('baguette', 'Bakery Products & Bread'),
            ('kuchen', 'Bakery Products & Bread'),
            ('biskuit', 'Bakery Products & Bread'),
            ('schokolade', 'Chocolate & Sweets'),
            ('bonbon', 'Chocolate & Sweets'),
            ('gummibärchen', 'Chocolate & Sweets'),
            ('cracker', 'Savory Snacks'),
            ('chips', 'Savory Snacks'),
            ('nüsse', 'Snacks & Sweets'),
            ('trockenfrüchte', 'Snacks & Sweets'),
            ('fleisch', 'Meat'),
            ('wurst', 'Meat'),
            ('meeresfrüchte', 'Fish & Seafood'),
            ('fisch', 'Fish & Seafood'),
            ('tiefgekühlt', 'Frozen Foods'),
            ('pizza', 'Frozen Foods'),
            ('baby-nahrung', 'Baby Products'),
            ('windeln', 'Baby Products'),
            ('babymilch', 'Baby Products'),
            ('waschmittel', 'Cleaning & Hygiene'),
            ('reiniger', 'Cleaning & Hygiene'),
            ('tandil', 'Cleaning & Hygiene'),
            ('seife', 'Personal Care'),
            ('shampoo', 'Personal Care'),
            ('zahnpasta', 'Personal Care'),
            ('geschirrspülmittel', 'Cleaning & Hygiene'),
            ('küchenzubehör', 'Household Supplies'),
            ('bbq', 'BBQ & Grilling'),
            ('salat', 'Salads'),
            ('nudeln', 'Pasta, Rice & Legumes'),
            # ('reis', 'Pasta, Rice & Legumes'),
            ('hülsenfrüchte', 'Pasta, Rice & Legumes'),
            ('suppe', 'Canned Goods'),
            ('müsli', 'Cereals & Bars'),
            ('Merlot', 'Wine'),
            ('haribo', 'Chocolate & Sweets'),
            ('macchiato', 'Tea & Coffee'),
            ('sauce', 'Sauces'),
            ('gnocchi', 'Pasta, Rice & Legumes'),
            ('sonnenblumenöl', 'Baking & Cooking'),
            ('babypflege', 'Baby Products'),
            ('tortelloni', 'Pasta, Rice & Legumes'),
            ('steinofenbrötchen', 'Bakery Products & Bread'),
            ('toastbrötchen', 'Bakery Products & Bread'),
            ('cremesso', 'Tea & Coffee'),
            ('magnesium', 'Health & Wellness'),
            ('spülmittel', 'Cleaning & Hygiene'),
            ('bitburger', 'Beer'),
            ('nivea', 'Personal Care'),
            ('axe', 'Personal Care'),
            ('ombia', 'Personal Care'),
            ('toilettenpapier', 'Personal Care'),
            ('limonade', 'Soft Drinks & Water'),
            ('mr beast', 'Chocolate & Sweets'),
            ('lacroix', 'Canned Goods'),
            ('maggi ravioli', 'Canned Goods'),
            ('riesenbohnen', 'Canned Goods'),
            ('mädchentraube', 'Wine'),
            ('clarkys','Savory Snacks'),
            ('cold brew', 'Soft Drinks & Water'),
            ('aperitivo', 'Spirits'),
            ('rührteig-tortenboden', 'Chocolate & Sweets'),
            ('frischer blätterteig', 'Bakery Products & Bread'),
            ('yogurette', 'Chocolate & Sweets'),
            ('kinder duo', 'Chocolate & Sweets'),
            ('pralinen', 'Chocolate & Sweets'),
            ('sauerkraut', 'Canned Goods'),
            ('meßmer', 'Tea & Coffee'),
            ('ritter sport', 'Chocolate & Sweets'),
            ('eisbecher', 'Frozen Foods'),
            ('milsani', 'Dairy Products'),
            ('eisriegel', 'Frozen Foods'),
            ('hofburger', 'Dairy Products'),
            ('ajvar', 'Sauces'),
            ('büffelmozzarella ', 'Dairy Products'),
            ('filtertüten', 'Tea & Coffee'),
        ]

        product_name_lower = product_name.lower()

        for keyword, refined_category in keyword_priority:
            if keyword in product_name_lower:
                return refined_category

        return original_category if original_category else 'Other'

    def map_category(self, store, category, product_name):
        """
        Maps the original category to a standardized category using both the
        category mapping and context-based keyword logic.
        """
        if isinstance(category, list):
            for cat in category:
                mapped_category = CATEGORY_MAPPING.get(store, {}).get(cat)
                if mapped_category:
                    return mapped_category
            return self.refine_category_with_context(product_name, 'Other')
        else:
            mapped_category = CATEGORY_MAPPING.get(store, {}).get(category, 'Other')
            return self.refine_category_with_context(product_name, mapped_category)

    def process_item(self, item, spider):
        try:
            item['id_tag'] = self.generate_id_tag(item['store'], item['name'])

            if 'category' in item:
                original_category = item['category']
                product_name = item.get('name', '')
                standardized_category = self.map_category(item['store'], original_category, product_name)
                item['category'] = standardized_category
            else:
                item['category'] = 'Other'

            return item
        except Exception as e:
            spider.logger.error(f"Error processing item: {e}")
            return item


class PostgresPipeline:

    def open_spider(self, spider):
        env = environs.Env()

        env.read_env(str(Path(__file__).resolve().parent.parent / '.env'))
        try:
            self.conn = psycopg.connect(
                dbname=env.str('DB_NAME'),
                user=env.str('DB_USER'),
                password=env.str('DB_PWD'),
                host=env.str('DB_HOST'),
                port=env.str('DB_PORT')
            )
            self.conn.autocommit = False  # Enable manual transaction control
            self.cursor = self.conn.cursor()
        except psycopg.OperationalError as e:
            spider.logger.error(f"PostgreSQL connection failed: {e}")
            raise scrapy.exceptions.NotConfigured("PostgreSQL connection failed")

    def close_spider(self, spider):
        # Close connection properly
        if hasattr(self, 'conn'):
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

    def process_item(self, item, spider):
        try:
            # Insert item into the PostgreSQL table
            insert_query = sql.SQL("""
                INSERT INTO product_product (store, name, price, currency, category, description, image, link, id_tag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """)
            values = (
                item.get('store'),
                item.get('name'),
                item.get('price'),
                item.get('currency'),
                item.get('category'),
                item.get('description'),
                item.get('image'),
                item.get('link'),
                item.get('id_tag'),
            )
            self.cursor.execute(insert_query, values)

        except psycopg.Error as e:
            spider.logger.error(f"Error processing item {item.get('name')}: {e}")
            self.conn.rollback()  # Rollback the transaction in case of error

        return item
