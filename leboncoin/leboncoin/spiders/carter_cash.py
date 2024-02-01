import os
from dotenv import load_dotenv
import pyodbc
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import subprocess
from datetime import datetime


# Charger les variables d'environnement


import os
from dotenv import load_dotenv
import pyodbc

# Charger les variables d'environnement
load_dotenv()
server = os.getenv('SERVER')
database = os.getenv('DATABASE')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

driver= '{ODBC Driver 17 for SQL Server}'

cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

def run_spider():
    subprocess.run(["scrapy", "crawl", "carter"])

class ImmoSpider(CrawlSpider):
    name = "carter"
    allowed_domains = ["www.carter-cash.com"]
    
    
    
    largeur =  {155: 155, 165: 165, 175: 175, 185: 185, 195: 195, 205: 205, 215: 215, 225: 225, 235: 235, 245: 245}
    hauteur =  {40: 40, 45: 45, 50: 50, 55: 55, 60: 60, 65: 65, 70: 70}
    diametre = {13:13,14:14,15:15,16:16,17:17,18:18,19:19}
    
    # largeur =  {175: 175}
    # hauteur =  {55: 55}
    # diametre = {15:15}

    start_urls = []
    for l in largeur.values():
        for h in hauteur.values():
            for d in diametre.values():
                url = f"https://www.carter-cash.com/pneus/{l}-{h}-r{d}"
                start_urls.append(url)
    
    film_details = LinkExtractor(restrict_css='h2 > a')
    rule_film_details = Rule(film_details,
                             callback='parse_item',
                             follow=False,
                             )
    rules = (rule_film_details,)

    user_agent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/111.0'
    
    

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers={'User-Agent': self.user_agent})


    def parse_item(self, response):
        # ////// les fonction de netoyage ///////////////////////
        descriptif = response.xpath('//h1[@class="name"]/text()').get()
        note = response.xpath('//div[@class="product-name"]/div/div/span[1]/text()[1]')
        runflat = response.xpath('//div[@id="features"]/ul/li[11]/span[2]/text()')
        consommation = response.xpath('//div[@id="features"]/ul/li[3]/span[2]/text()').get()
        charge = response.xpath('//div[@id="features"]/ul/li[9]/span[2]/text()').get()
        vitesse = response.xpath('//div[@id="features"]/ul/li[10]/span[2]/text()').get()
        indice_pluie = response.xpath('//div[@id="features"]/ul/li[4]/span[2]/text()').get()

        item = {}
        item["Prix"] = response.xpath('//*[@id="tire"]/div[2]/div[3]/div/div[3]/div[2]/div[2]/div[1]/form/div[1]/div[1]/div/div/span/text()').get()
        item["url-produit"] = response.url

        item["Info_generale"] = response.xpath('//h1/div/text()').get().strip()
        item["Descriptif"] = ' '.join(descriptif.split()[1:])
        
        item["Saisonalite"] = response.xpath('//div[@id="features"]/ul/li[1]/span[2]/text()').get().strip()
        item["Type_Vehicule"] = response.xpath('//div[@id="features"]/ul/li[2]/span[2]/text()').get().strip()
     # ////// 
        item["Consommation"] = consommation if consommation and consommation.isalpha() and ('a' <= consommation.lower() <= 'f') else ""
    # ////// 
        
        item["Indice_Pluie"] = indice_pluie if indice_pluie and indice_pluie.isalpha() and ('a' <= indice_pluie.lower() <= 'f') else ""

    # //////        
        # item["Bruit"] = response.xpath('//div[@id="features"]/ul/li[5]/span[2]/text()').get()
        # if item["Bruit"] and not item["Bruit"].endswith(""):
        #     item["Bruit"] = ""
            
        bruit = response.xpath('//div[@id="features"]/ul/li[5]/span[2]/text()').get()
        if bruit:
            item["Bruit"] = bruit.rstrip(" db")
        else:
            item["Bruit"] = ""
            
        url_parts = response.url.split("/")
        tire_dimensions = url_parts[-1].split("-")
        item["Largeur"] = tire_dimensions[0]
        item["Hauteur"] = tire_dimensions[1].split("r")[0]
        item["Diametre"] = tire_dimensions[1].split("r")[1]        
        
        if charge and charge.isnumeric() and 1 <= int(charge) <= 100:
            item["Charge"] = charge
        else:
            item["Charge"] = ""
    # ////// 
        
        if vitesse and vitesse.isalpha() and 'a' <= vitesse.lower() <= 'z':
            item["Vitesse"] = vitesse
        else:
            item["Vitesse"] = ""
       # //////      
            
        if runflat:
            item["Runflat"] = runflat.get()
        else:
            item["Runflat"] = ""     
            
        # //////         
        if note:
            note_text = note.get().split('/')[0]
            if "reconditionné" not in note_text:
                item["Note"] = note_text
            else:
                item["Note"] = "note inconnue"
        else:
            item["Note"] = "note inconnue"
        
        item["Date_scrap"] = datetime.now().strftime('%Y-%m-%d')



        # Insérer les données dans la base de données
        # print(f"Inserting into Produit: {item['url-produit'],item['Prix'], item['Info_generale'], item['Descriptif'], item['Saisonalite'], item['Type_Vehicule'], item['Runflat'], item['Note'], item['Date_scrap']}")
        cursor.execute("""
        INSERT INTO Produit (URL_Produit, Prix, Info_generale, Descriptif, Note, Date_scrap)
        VALUES (?, ?, ?, ?, ?, ?)
        """, item["url-produit"], item["Prix"], item["Info_generale"], item["Descriptif"], item["Note"], item["Date_scrap"])

        # Récupérer l'ID du produit inséré
        ID_Produit = cursor.execute("SELECT @@IDENTITY AS ID").fetchone()[0]

        # print(f"Inserting into Prix: {item['Prix'], ID_Produit}")
        # cursor.execute("""
        # INSERT INTO Prix (Prix, ID_Produit)
        # VALUES (?, ?)
        # """, item["Prix"], ID_Produit)

        # print(f"Inserting into Caracteristiques: {item['Consommation'], item['Indice_Pluie'], item['Bruit'], ID_Produit}")
        cursor.execute("""
        INSERT INTO Caracteristiques (Consommation, Indice_Pluie, Bruit, ID_Produit, Saisonalite, Type_Vehicule, Runflat)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, item["Consommation"], item["Indice_Pluie"], item["Bruit"], ID_Produit, item["Saisonalite"], item["Type_Vehicule"], item["Runflat"])

        # print(f"Inserting into Dimensions: {item['Largeur'], item['Hauteur'], item['Diametre'], item['Charge'], item['Vitesse'], ID_Produit}")
        cursor.execute("""
        INSERT INTO Dimensions (Largeur, Hauteur, Diametre, Charge, Vitesse, ID_Produit)
        VALUES (?, ?, ?, ?, ?, ?)
        """, item["Largeur"], item["Hauteur"], item["Diametre"], item["Charge"], item["Vitesse"], ID_Produit)

        cnxn.commit()


        return item

run_spider()