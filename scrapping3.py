from kaggle.api.kaggle_api_extended import KaggleApi
import requests
from bs4 import BeautifulSoup
import json
import os
from rdflib import Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS, XSD
from urllib.parse import quote

# Inisialisasi RDF Graph dan namespace
g = Graph()
EX = Namespace("http://example.org/ontology#")
DCAT = Namespace("http://www.w3.org/ns/dcat#")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")
SCHEMA = Namespace("http://schema.org/")

g.bind("ex", EX)
g.bind("dcat", DCAT)
g.bind("foaf", FOAF)
g.bind("schema", SCHEMA)

# Inisialisasi Kaggle API
api = KaggleApi()
api.authenticate()

# Daftar dataset Kaggle yang ingin diambil
dataset_refs = [
    'kekavigi/earthquakes-in-indonesia',
    'muamkh/ihsgstockdata', 
    'rezkyyayang/pekerja-sejahtera',
    'ardikasatria/datasettanamanpadisumatera',
    'ilhamfp31/indonesian-abusive-and-hate-speech-twitter-text',
    'muhammadhabibna/hospital-data-in-indonesia',
    'safrizalardanaa/produk-ecommerce-indonesia',
    'achmadnoer/alfabet-bisindo',
    'linkgish/indonesian-salary-by-region-19972022',
    'pramudyadika/yogyakarta-housing-price-ndonesia',
    'tiwill/saham-idx',
    'ermila/klasifikasi-tingkat-kemiskinan-di-indonesia',
    'thedevastator/tourists-attractions-in-indonesia',
    'anandhuh/population-indonasia',
    'adhang/air-quality-in-yogyakarta-indonesia-2021',
    'imamdigmi/indonesian-plate-number',
    'anggagewor/data-wilayah-republic-indonesia',
    'sh1zuka/indonesia-news-dataset-2024',
    'gantisumpah/indonesian-local-perfume-brand',
    'ariffaizin/indonesia-election-news-berita-pemilu-2024'
]

def scrape_kaggle_metadata(ref):
    """Melakukan scraping metadata dari halaman dataset Kaggle"""
    url = f"https://www.kaggle.com/{ref}"
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Ambil title
        title_tag = soup.find('title')
        title = title_tag.text.split('|')[0].strip() if title_tag else 'N/A'
        
        # Ambil description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        description = meta_desc.get('content', 'N/A') if meta_desc else 'N/A'
        
        # Ambil keywords (sebagai kategori)
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        categories = meta_keywords.get('content', 'N/A') if meta_keywords else 'N/A'
        
        # Ambil owner dari URL
        owner = ref.split('/')[0]
        
        return {
            'title': title,
            'description': description,
            'categories': categories,
            'owner': owner
        }
        
    except Exception as e:
        return {
            'title': 'N/A',
            'description': 'N/A',
            'categories': 'N/A',
            'owner': 'N/A'
        }

def add_to_rdf(ref, title, description, categories, owner, formats):
    """Menambahkan informasi ke dalam RDF graph"""
    # Buat URI untuk dataset
    dataset_uri = URIRef(f"https://kaggle.com/{ref}")
    
    # Tambahkan tipe dasar
    g.add((dataset_uri, RDF.type, DCAT.Dataset))
    g.add((dataset_uri, RDF.type, SCHEMA.Dataset))
    
    # Tambahkan properti utama
    g.add((dataset_uri, DCAT.title, Literal(title, lang="id")))
    g.add((dataset_uri, DCAT.description, Literal(description, lang="id")))
    g.add((dataset_uri, DCAT.creator, Literal(owner)))
    
    # Tambahkan format
    for fmt in formats.split(", "):
        if fmt != "N/A":
            g.add((dataset_uri, DCAT.mediaType, Literal(fmt)))
    
    # Tambahkan kategori (lakukan URL encoding untuk kategori)
    if categories != "N/A":
        for category in categories.split(","):
            # Lakukan URL encoding pada kategori agar valid
            encoded_category = quote(category.strip().lower())
            category_uri = URIRef(f"http://example.org/category/{encoded_category}")
            g.add((dataset_uri, DCAT.theme, category_uri))
            g.add((category_uri, RDFS.label, Literal(category.strip())))
    
    # Tambahkan distribusi file
    distribution_uri = URIRef(f"https://kaggle.com/{ref}/distribution")
    g.add((dataset_uri, DCAT.distribution, distribution_uri))
    g.add((distribution_uri, RDF.type, DCAT.Distribution))
    g.add((distribution_uri, DCAT.downloadURL, URIRef(f"https://kaggle.com/{ref}/download")))

# Loop untuk setiap dataset
for ref in dataset_refs:
    try:
        # Coba ambil via API
        metadata_path = './temp_metadata'
        os.makedirs(metadata_path, exist_ok=True)
        
        # Ambil metadata API
        api.dataset_metadata(ref, path=metadata_path)
        
        # Baca file metadata
        metadata_file = os.path.join(metadata_path, 'dataset-metadata.json')
        with open(metadata_file, 'r', encoding='utf-8') as f:
            api_data = json.load(f)
        
        # Format data API
        title = api_data.get("title", "N/A")
        description = api_data.get("description", "N/A")
        categories = api_data.get("categories", "N/A")
        owner = api_data.get("owner", {}).get("name", ref.split('/')[0])
        
        # Ambil format file
        files = api.dataset_list_files(ref).files
        formats = {file.name.split(".")[-1].upper() for file in files}
        formats_str = ", ".join(formats) if formats else "N/A"
        
    except Exception as e:
        # Jika API gagal, gunakan scraping
        print(f"Error API untuk {ref}: {str(e)}")
        scraped_data = scrape_kaggle_metadata(ref)
        title = scraped_data['title']
        description = scraped_data['description']
        categories = scraped_data['categories']
        owner = scraped_data['owner']
        
        # Ambil format file via scraping
        formats_str = "N/A"
        try:
            files = api.dataset_list_files(ref).files
            formats = {file.name.split(".")[-1].upper() for file in files}
            formats_str = ", ".join(formats) if formats else "N/A"
        except:
            pass
    
    # Tampilkan hasil
    print(f"Metadata untuk {ref}:")
    print(f"Judul: {title}")
    print(f"Deskripsi: {description}")
    print(f"Kategori: {categories}")
    print(f"Pemilik: {owner}")
    print(f"Format File: {formats_str}")
    print("-" * 50)
    
    # Setelah mendapatkan metadata, tambahkan ke RDF
    add_to_rdf(
        ref,
        title,
        description,
        categories,
        owner,
        formats_str
    )

# Serialisasi RDF ke format Turtle
rdf_output = g.serialize(format="turtle", encoding="utf-8")
with open("kaggle_data.ttl", "wb") as f:
    f.write(rdf_output)

print("Konversi ke RDF selesai! Output tersimpan di kaggle_data.ttl")