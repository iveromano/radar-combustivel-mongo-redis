"""
Seed do Radar Combustivel com cidades, UFs e bairros REAIS do Brasil.
====================================================================

Gera dados sinteticos mas geograficamente coerentes:
  * UFs reais (todas as 27)
  * Cidades reais com coordenadas aproximadas oficiais
  * Bairros plausiveis (mistura de bairros conhecidos + sintetizados)
  * Coordenadas de cada posto sao geradas com pequeno deslocamento
    em torno do centro da cidade, de modo que GEOSEARCH retorne
    resultados coerentes com o estado/cidade do cadastro.

Volume gerado (alvo, configuravel via CLI). Defaults somam 500k docs:
  * postos                  : 100.000
  * localizacoes_postos     : 100.000  (1 por posto)
  * eventos_preco           : 100.000
  * buscas_usuarios         : 100.000
  * avaliacoes_interacoes   : 100.000

Uso:
    python init/seed.py                  # roda com defaults
    python init/seed.py --drop           # apaga colecoes antes
    python init/seed.py --postos 5000    # ajusta volume
"""
from __future__ import annotations

import argparse
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from bson import ObjectId
from pymongo import MongoClient

# Permite executar como script ou modulo
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import (  # noqa: E402
    COL_AVALIACOES,
    COL_BUSCAS,
    COL_LOCALIZACOES,
    COL_POSTOS,
    COL_PRECOS,
    MONGO_DB,
    MONGO_URI,
    configure_logging,
)


log = configure_logging("seed")

SEED = int(os.getenv("SEED", "42"))
random.seed(SEED)


# ===========================================================================
# Dataset geografico do Brasil
# ===========================================================================
# Cidades reais com (lat, lon) e bairros conhecidos. Para cidades grandes
# usamos bairros reais; para as demais usamos bairros plausiveis.

CIDADES = [
    # =============== SP - 22 cidades ===============
    ("SP", "Sao Paulo", "3550308", -23.5505, -46.6333,
     ["Pinheiros", "Vila Madalena", "Mooca", "Tatuape", "Santana",
      "Itaim Bibi", "Liberdade", "Bela Vista", "Lapa", "Brooklin",
      "Vila Mariana", "Perdizes", "Ipiranga", "Campo Belo", "Moema",
      "Jardins", "Vila Olimpia", "Saude", "Santo Amaro", "Aclimacao"]),
    ("SP", "Campinas", "3509502", -22.9099, -47.0626,
     ["Cambui", "Centro", "Barao Geraldo", "Taquaral", "Mansoes Santo Antonio"]),
    ("SP", "Guarulhos", "3518800", -23.4628, -46.5333,
     ["Centro", "Macedo", "Vila Galvao", "Taboao", "Pimentas"]),
    ("SP", "Sao Bernardo do Campo", "3548708", -23.6914, -46.5646,
     ["Centro", "Rudge Ramos", "Anchieta", "Baeta Neves"]),
    ("SP", "Santo Andre", "3547809", -23.6633, -46.5306,
     ["Centro", "Vila Assuncao", "Jardim", "Casa Branca"]),
    ("SP", "Osasco", "3534401", -23.5328, -46.7917,
     ["Centro", "Vila Yara", "City Bussocaba", "Km 18"]),
    ("SP", "Santos", "3548500", -23.9608, -46.3331,
     ["Gonzaga", "Boqueirao", "Embare", "Aparecida", "Ponta da Praia"]),
    ("SP", "Sao Jose dos Campos", "3549904", -23.2237, -45.9009,
     ["Jardim Aquarius", "Centro", "Urbanova", "Jardim Apolo"]),
    ("SP", "Sorocaba", "3552205", -23.5015, -47.4526,
     ["Centro", "Campolim", "Jardim Europa", "Vila Trujillo"]),
    ("SP", "Ribeirao Preto", "3543402", -21.1775, -47.8103,
     ["Jardim Sumare", "Vila Tiberio", "Centro", "Ribeirania"]),
    ("SP", "Sao Jose do Rio Preto", "3549805", -20.8113, -49.3758,
     ["Centro", "Higienopolis", "Boa Vista", "Vila Imperial"]),
    ("SP", "Bauru", "3506003", -22.3145, -49.0608,
     ["Centro", "Vila Souto", "Jardim Estoril", "Altos da Cidade"]),
    ("SP", "Piracicaba", "3538709", -22.7253, -47.6492,
     ["Centro", "Cidade Alta", "Sao Dimas", "Higienopolis"]),
    ("SP", "Mogi das Cruzes", "3530607", -23.5210, -46.1858,
     ["Centro", "Mogilar", "Vila Oliveira", "Brasal"]),
    ("SP", "Diadema", "3513801", -23.6857, -46.6232,
     ["Centro", "Eldorado", "Piraporinha", "Casa Grande"]),
    ("SP", "Jundiai", "3525904", -23.1864, -46.8842,
     ["Centro", "Anhangabau", "Vianelo", "Jardim Ana Maria"]),
    ("SP", "Carapicuiba", "3510609", -23.5224, -46.8359,
     ["Centro", "Cohab", "Aldeia da Serra", "Vila Dirce"]),
    ("SP", "Limeira", "3526902", -22.5648, -47.4015,
     ["Centro", "Vila Claudia", "Boa Vista", "Vista Alegre"]),
    ("SP", "Taubate", "3554102", -23.0264, -45.5553,
     ["Centro", "Jardim das Nacoes", "Vila Sao Geraldo", "Independencia"]),
    ("SP", "Praia Grande", "3541000", -24.0058, -46.4028,
     ["Boqueirao", "Tupi", "Aviacao", "Vila Tupi"]),
    ("SP", "Maua", "3529401", -23.6678, -46.4614,
     ["Centro", "Vila Magini", "Itapeva", "Jardim Zaira"]),
    ("SP", "Marilia", "3529005", -22.2139, -49.9461,
     ["Centro", "Palmital", "Maracana", "Senador Salgado"]),

    # =============== RJ - 11 cidades ===============
    ("RJ", "Rio de Janeiro", "3304557", -22.9068, -43.1729,
     ["Copacabana", "Ipanema", "Leblon", "Botafogo", "Tijuca",
      "Barra da Tijuca", "Flamengo", "Centro", "Lapa", "Recreio dos Bandeirantes",
      "Madureira", "Meier", "Vila Isabel", "Laranjeiras", "Santa Teresa",
      "Jacarepagua", "Bangu", "Campo Grande"]),
    ("RJ", "Niteroi", "3303302", -22.8836, -43.1037,
     ["Icarai", "Centro", "Sao Francisco", "Charitas", "Ingá"]),
    ("RJ", "Petropolis", "3303906", -22.5113, -43.1855,
     ["Centro", "Itaipava", "Quitandinha", "Bingen"]),
    ("RJ", "Nova Iguacu", "3303500", -22.7592, -43.4511,
     ["Centro", "Vila de Cava", "Comendador Soares"]),
    ("RJ", "Duque de Caxias", "3301702", -22.7858, -43.3115,
     ["Centro", "Saracuruna", "Imbarie", "Sarapui"]),
    ("RJ", "Sao Goncalo", "3304904", -22.8270, -43.0539,
     ["Centro", "Alcantara", "Mutua", "Trindade"]),
    ("RJ", "Belford Roxo", "3300456", -22.7642, -43.3994,
     ["Centro", "Heliopolis", "Sargento Roncalli"]),
    ("RJ", "Campos dos Goytacazes", "3301009", -21.7622, -41.3181,
     ["Centro", "Parque Califórnia", "Pelinca", "Goitacazes"]),
    ("RJ", "Volta Redonda", "3306305", -22.5236, -44.1042,
     ["Aterrado", "Centro", "Jardim Amalia", "Bela Vista"]),
    ("RJ", "Sao Joao de Meriti", "3305109", -22.8042, -43.3722,
     ["Centro", "Sao Mateus", "Coelho da Rocha", "Vilar dos Telles"]),
    ("RJ", "Magé", "3302601", -22.6533, -43.0408,
     ["Centro", "Vila Nova", "Suruí", "Citrolândia"]),

    # =============== MG - 14 cidades ===============
    ("MG", "Belo Horizonte", "3106200", -19.9167, -43.9345,
     ["Savassi", "Funcionarios", "Lourdes", "Pampulha", "Centro",
      "Buritis", "Belvedere", "Santa Tereza", "Cidade Nova", "Anchieta",
      "Sion", "Coracao Eucaristico", "Floresta", "Sao Pedro"]),
    ("MG", "Uberlandia", "3170206", -18.9113, -48.2622,
     ["Centro", "Santa Monica", "Tibery", "Patrimonio"]),
    ("MG", "Contagem", "3118601", -19.9320, -44.0539,
     ["Eldorado", "Industrial", "Riacho das Pedras"]),
    ("MG", "Juiz de Fora", "3136702", -21.7642, -43.3503,
     ["Centro", "Sao Mateus", "Cascatinha", "Aeroporto"]),
    ("MG", "Betim", "3106705", -19.9678, -44.1981,
     ["Centro", "Angola", "Citrolandia", "Filadelfia"]),
    ("MG", "Pouso Alegre", "3152501", -22.2308, -45.9367,
     ["Centro", "Sao Geraldo", "Cidade Jardim", "Industrial",
      "Aeroporto", "Cruz das Posses", "Sao Joao", "Sao Cristovao"]),
    ("MG", "Itajuba", "3132404", -22.4255, -45.4521,
     ["Centro", "Anhumas", "Boa Vista", "Sao Vicente",
      "Pinheirinho", "Estiva", "Morro Chic", "Varginha"]),
    ("MG", "Uberaba", "3170107", -19.7472, -47.9381,
     ["Centro", "Fabricio", "Mercês", "Sao Benedito"]),
    ("MG", "Montes Claros", "3143302", -16.7286, -43.8617,
     ["Centro", "Major Prates", "Maracana", "Bela Vista"]),
    ("MG", "Ipatinga", "3131307", -19.4682, -42.5371,
     ["Centro", "Bom Retiro", "Cidade Nobre", "Veneza"]),
    ("MG", "Sete Lagoas", "3167202", -19.4658, -44.2467,
     ["Centro", "Boa Vista", "Eldorado", "Jardim Cambui"]),
    ("MG", "Divinopolis", "3122306", -20.1389, -44.8917,
     ["Centro", "Bom Pastor", "Niteroi", "Sao Jose"]),
    ("MG", "Varginha", "3170701", -21.5519, -45.4361,
     ["Centro", "Vila Pinto", "Vila Verde", "Sao Sebastiao"]),
    ("MG", "Pocos de Caldas", "3151800", -21.7869, -46.5611,
     ["Centro", "Jardim dos Estados", "Sao Benedito", "Vila Cascatinha"]),

    # =============== ES - 5 cidades ===============
    ("ES", "Vitoria", "3205309", -20.3155, -40.3128,
     ["Centro", "Praia do Canto", "Jardim Camburi", "Bento Ferreira", "Jardim da Penha"]),
    ("ES", "Vila Velha", "3205200", -20.3414, -40.2876,
     ["Centro", "Praia da Costa", "Itapua", "Glória"]),
    ("ES", "Serra", "3205002", -20.1281, -40.3078,
     ["Laranjeiras", "Jacaraipe", "Eldorado", "Manguinhos"]),
    ("ES", "Cariacica", "3201308", -20.2625, -40.4203,
     ["Centro", "Campo Grande", "Itacibá", "Jardim America"]),
    ("ES", "Cachoeiro de Itapemirim", "3201209", -20.8489, -41.1128,
     ["Centro", "Independencia", "Maria Ortiz", "Aeroporto"]),

    # =============== RS - 9 cidades ===============
    ("RS", "Porto Alegre", "4314902", -30.0346, -51.2177,
     ["Moinhos de Vento", "Cidade Baixa", "Bom Fim", "Centro Historico",
      "Petropolis", "Higienopolis", "Boa Vista", "Menino Deus", "Bela Vista"]),
    ("RS", "Caxias do Sul", "4305108", -29.1678, -51.1794,
     ["Centro", "Sao Pelegrino", "Cinquentenario", "Marechal Floriano"]),
    ("RS", "Pelotas", "4314407", -31.7654, -52.3376,
     ["Centro", "Tres Vendas", "Areal", "Fragata"]),
    ("RS", "Canoas", "4304606", -29.9214, -51.1814,
     ["Centro", "Niteroi", "Mathias Velho", "Igara"]),
    ("RS", "Santa Maria", "4316907", -29.6842, -53.8069,
     ["Centro", "Camobi", "Itarare", "Nossa Senhora de Fatima"]),
    ("RS", "Gravatai", "4309209", -29.9442, -50.9925,
     ["Centro", "Parque dos Eucaliptos", "Bom Sucesso", "Salgado Filho"]),
    ("RS", "Viamao", "4323002", -30.0808, -51.0233,
     ["Centro", "Tarumã", "Vila Augusta", "Santa Isabel"]),
    ("RS", "Novo Hamburgo", "4313409", -29.6783, -51.1306,
     ["Centro", "Hamburgo Velho", "Sao Jose", "Rondonia"]),
    ("RS", "Rio Grande", "4315602", -32.0350, -52.0986,
     ["Centro", "Cidade Nova", "Cassino", "Vila da Quinta"]),

    # =============== PR - 9 cidades ===============
    ("PR", "Curitiba", "4106902", -25.4284, -49.2733,
     ["Batel", "Centro", "Agua Verde", "Boa Vista", "Cabral",
      "Champagnat", "Mercês", "Bigorrilho", "Juveve", "Portao", "Cristo Rei"]),
    ("PR", "Londrina", "4113700", -23.3045, -51.1696,
     ["Centro", "Gleba Palhano", "Agari", "Igapó"]),
    ("PR", "Maringa", "4115200", -23.4253, -51.9382,
     ["Zona 7", "Centro", "Vila Operaria", "Vila Esperanca"]),
    ("PR", "Foz do Iguacu", "4108304", -25.5478, -54.5882,
     ["Centro", "Vila A", "Jardim Iguacu", "Vila Yolanda"]),
    ("PR", "Cascavel", "4104808", -24.9555, -53.4552,
     ["Centro", "Coqueiral", "Sao Cristovao", "Pioneiros Catarinenses"]),
    ("PR", "Ponta Grossa", "4119905", -25.0950, -50.1619,
     ["Centro", "Uvaranas", "Olarias", "Estrela"]),
    ("PR", "Sao Jose dos Pinhais", "4125506", -25.5346, -49.2061,
     ["Centro", "Afonso Pena", "Borda do Campo", "Cidade Jardim"]),
    ("PR", "Colombo", "4105805", -25.2917, -49.2236,
     ["Centro", "Sao Gabriel", "Atuba", "Maracanã"]),
    ("PR", "Guarapuava", "4109401", -25.3950, -51.4569,
     ["Centro", "Trianon", "Boqueirao", "Bonsucesso"]),

    # =============== SC - 8 cidades ===============
    ("SC", "Florianopolis", "4205407", -27.5949, -48.5482,
     ["Centro", "Lagoa da Conceicao", "Trindade", "Coqueiros",
      "Itacorubi", "Ingleses", "Canasvieiras", "Campeche"]),
    ("SC", "Joinville", "4209102", -26.3045, -48.8487,
     ["Centro", "America", "Bucarein", "Atiradores"]),
    ("SC", "Blumenau", "4202404", -26.9194, -49.0661,
     ["Centro", "Velha", "Garcia", "Itoupava"]),
    ("SC", "Sao Jose", "4216602", -27.6131, -48.6358,
     ["Centro", "Kobrasol", "Forquilhinhas", "Campinas"]),
    ("SC", "Chapeco", "4204202", -27.0989, -52.6181,
     ["Centro", "Sao Cristovao", "Jardim do Lago", "Efapi"]),
    ("SC", "Itajai", "4208203", -26.9081, -48.6619,
     ["Centro", "Cabeçudas", "Fazenda", "Sao Vicente"]),
    ("SC", "Criciuma", "4204608", -28.6775, -49.3697,
     ["Centro", "Pinheirinho", "Sao Cristovao", "Proeza"]),
    ("SC", "Lages", "4209300", -27.8167, -50.3261,
     ["Centro", "Habitacao", "Coral", "Conta Dinheiro"]),

    # =============== BA - 8 cidades ===============
    ("BA", "Salvador", "2927408", -12.9714, -38.5014,
     ["Pituba", "Barra", "Ondina", "Itaigara", "Caminho das Arvores",
      "Pelourinho", "Rio Vermelho", "Graca", "Itapagipe", "Brotas",
      "Federacao", "Stiep", "Bonfim"]),
    ("BA", "Feira de Santana", "2910800", -12.2664, -38.9663,
     ["Centro", "Brasilia", "Capucho", "Kalilandia"]),
    ("BA", "Vitoria da Conquista", "2933307", -14.8615, -40.8442,
     ["Centro", "Candeias", "Recreio", "Universidade"]),
    ("BA", "Camacari", "2905701", -12.6975, -38.3242,
     ["Centro", "Gravata", "Phoc", "Lama Preta"]),
    ("BA", "Itabuna", "2914802", -14.7858, -39.2803,
     ["Centro", "Sao Caetano", "Sao Pedro", "Conceicao"]),
    ("BA", "Juazeiro", "2918407", -9.4111, -40.5067,
     ["Centro", "Maringa", "Antonio Guilhermino", "Joao Paulo II"]),
    ("BA", "Lauro de Freitas", "2919207", -12.8944, -38.3222,
     ["Centro", "Vilas do Atlantico", "Itinga", "Buraquinho"]),
    ("BA", "Ilheus", "2913606", -14.7889, -39.0481,
     ["Centro", "Pontal", "Nelson Costa", "Malhado"]),

    # =============== PE - 6 cidades ===============
    ("PE", "Recife", "2611606", -8.0476, -34.8770,
     ["Boa Viagem", "Pina", "Casa Forte", "Rosarinho", "Espinheiro",
      "Boa Vista", "Aflitos", "Madalena", "Encruzilhada", "Casa Amarela",
      "Torre", "Iputinga"]),
    ("PE", "Jaboatao dos Guararapes", "2607901", -8.1130, -35.0150,
     ["Piedade", "Candeias", "Centro", "Cavaleiro"]),
    ("PE", "Olinda", "2609600", -8.0084, -34.8553,
     ["Centro", "Casa Caiada", "Bairro Novo", "Rio Doce"]),
    ("PE", "Caruaru", "2604106", -8.2826, -35.9750,
     ["Centro", "Mauricio de Nassau", "Universitario"]),
    ("PE", "Petrolina", "2611101", -9.3891, -40.5030,
     ["Centro", "Cohab Massangano", "Vila Eduardo", "Areia Branca"]),
    ("PE", "Paulista", "2610707", -7.9408, -34.8728,
     ["Centro", "Maranguape", "Janga", "Pau Amarelo"]),

    # =============== CE - 5 cidades ===============
    ("CE", "Fortaleza", "2304400", -3.7319, -38.5267,
     ["Aldeota", "Meireles", "Centro", "Cocó", "Praia de Iracema",
      "Mucuripe", "Edson Queiroz", "Papicu", "Varjota", "Joaquim Tavora"]),
    ("CE", "Caucaia", "2303709", -3.7361, -38.6531,
     ["Centro", "Tabapua", "Icarai", "Jurema"]),
    ("CE", "Maracanau", "2307650", -3.8769, -38.6256,
     ["Centro", "Pajucara", "Jereissati", "Acaracuzinho"]),
    ("CE", "Sobral", "2312908", -3.6889, -40.3500,
     ["Centro", "Coelce", "Junco", "Dom Expedito"]),
    ("CE", "Juazeiro do Norte", "2307304", -7.2128, -39.3158,
     ["Centro", "Sao Jose", "Triangulo", "Sao Miguel"]),

    # =============== DF / MA / PI / RN / PB / AL / SE ===============
    ("DF", "Brasilia", "5300108", -15.7942, -47.8822,
     ["Asa Sul", "Asa Norte", "Lago Sul", "Lago Norte", "Sudoeste",
      "Octogonal", "Plano Piloto", "Cruzeiro", "Aguas Claras", "Taguatinga",
      "Ceilândia", "Guara"]),
    ("MA", "Sao Luis", "2111300", -2.5307, -44.3068,
     ["Centro", "Cohama", "Renascenca", "Olho d'Agua", "Calhau"]),
    ("MA", "Imperatriz", "2105302", -5.5258, -47.4928,
     ["Centro", "Maranhao Novo", "Bacuri", "Joao Lisboa"]),
    ("MA", "Sao Jose de Ribamar", "2111201", -2.5614, -44.0517,
     ["Centro", "Araçagy", "Cohatrac", "Pirâmide"]),
    ("PI", "Teresina", "2211001", -5.0892, -42.8019,
     ["Centro", "Jockey", "Fatima", "Saci", "Horto"]),
    ("PI", "Parnaiba", "2207702", -2.9039, -41.7767,
     ["Centro", "Pindorama", "Ilhotas", "Sao Vicente"]),
    ("RN", "Natal", "2408102", -5.7945, -35.2110,
     ["Ponta Negra", "Tirol", "Petropolis", "Lagoa Nova", "Centro", "Capim Macio"]),
    ("RN", "Mossoro", "2408003", -5.1875, -37.3444,
     ["Centro", "Alto de Sao Manoel", "Bom Jardim", "Aeroporto"]),
    ("RN", "Parnamirim", "2403251", -5.9156, -35.2628,
     ["Centro", "Nova Parnamirim", "Cohabinal", "Emaus"]),
    ("PB", "Joao Pessoa", "2507507", -7.1195, -34.8450,
     ["Tambau", "Manaira", "Cabo Branco", "Centro", "Bessa", "Bancarios"]),
    ("PB", "Campina Grande", "2504009", -7.2306, -35.8811,
     ["Centro", "Catolé", "Bodocongó", "Prata"]),
    ("AL", "Maceio", "2704302", -9.6498, -35.7089,
     ["Pajucara", "Ponta Verde", "Jatiuca", "Mangabeiras", "Centro", "Jaragua"]),
    ("AL", "Arapiraca", "2700300", -9.7522, -36.6611,
     ["Centro", "Brasilia", "Eldorado", "Massaranduba"]),
    ("SE", "Aracaju", "2800308", -10.9472, -37.0731,
     ["Atalaia", "Treze de Julho", "Centro", "Salgado Filho", "Jardins"]),
    ("SE", "Nossa Senhora do Socorro", "2804409", -10.8550, -37.1264,
     ["Centro", "Taiçoca", "Marcos Freire", "Joao Alves"]),

    # =============== Centro-Oeste ===============
    ("GO", "Goiania", "5208707", -16.6869, -49.2648,
     ["Setor Bueno", "Setor Marista", "Setor Oeste", "Centro", "Setor Sul",
      "Setor Aeroporto", "Jardim Goias", "Setor Pedro Ludovico"]),
    ("GO", "Aparecida de Goiania", "5201405", -16.8233, -49.2436,
     ["Centro", "Vila Brasilia", "Jardim Olimpico", "Buena Vista"]),
    ("GO", "Anapolis", "5201108", -16.3267, -48.9527,
     ["Centro", "Jundiai", "Vila Brasilia", "Maracanã"]),
    ("GO", "Luziania", "5212501", -16.2528, -47.9503,
     ["Centro", "Mingone", "Sao Bernardo", "Park Sao Pedro"]),
    ("MT", "Cuiaba", "5103403", -15.5989, -56.0949,
     ["Centro", "Goiabeiras", "Coxipó", "Areao"]),
    ("MT", "Varzea Grande", "5108402", -15.6464, -56.1325,
     ["Centro", "Cristo Rei", "Mapim", "Costa Verde"]),
    ("MT", "Rondonopolis", "5107602", -16.4706, -54.6358,
     ["Centro", "Vila Aurora", "Sao Jose", "Vila Birigui"]),
    ("MS", "Campo Grande", "5002704", -20.4486, -54.6295,
     ["Centro", "Itanhanga", "Carandá", "Tiradentes", "Monte Castelo"]),
    ("MS", "Dourados", "5003702", -22.2208, -54.8061,
     ["Centro", "Jardim America", "Vila Industrial", "Jardim Agua Boa"]),
    ("MS", "Tres Lagoas", "5008305", -20.7872, -51.7039,
     ["Centro", "Vila Piloto", "Vila Nova", "Jardim Alvorada"]),

    # =============== Norte ===============
    ("AM", "Manaus", "1302603", -3.1190, -60.0217,
     ["Adrianopolis", "Centro", "Compensa", "Flores", "Aleixo", "Parque 10",
      "Sao Jose", "Cidade Nova", "Petropolis"]),
    ("AM", "Parintins", "1303205", -2.6283, -56.7331,
     ["Centro", "Itauna", "Sao Jose", "Sao Cristovao"]),
    ("PA", "Belem", "1501402", -1.4558, -48.5039,
     ["Nazaré", "Umarizal", "Batista Campos", "Reduto", "Cidade Velha", "Pedreira"]),
    ("PA", "Ananindeua", "1500800", -1.3658, -48.3725,
     ["Centro", "Cidade Nova", "Coqueiro", "Aurá"]),
    ("PA", "Santarem", "1506807", -2.4439, -54.7081,
     ["Centro", "Aldeia", "Salé", "Caranazal"]),
    ("PA", "Maraba", "1504208", -5.3686, -49.1175,
     ["Centro", "Nova Maraba", "Cidade Nova", "Cidade Jardim"]),
    ("TO", "Palmas", "1721000", -10.2128, -48.3603,
     ["Plano Diretor Norte", "Plano Diretor Sul", "Aureny", "Taquaralto"]),
    ("TO", "Araguaina", "1702109", -7.1919, -48.2072,
     ["Centro", "Vila Couto Magalhaes", "Setor Brasil", "Setor Universitario"]),
    ("RO", "Porto Velho", "1100205", -8.7619, -63.9039,
     ["Centro", "Olaria", "Areal", "Industrial"]),
    ("RO", "Ji-Parana", "1100122", -10.8853, -61.9519,
     ["Centro", "Sao Bernardo", "Nova Brasilia", "Urupa"]),
    ("RO", "Ariquemes", "1100023", -9.9133, -63.0408,
     ["Centro", "Setor 01", "Jardim Jorge Teixeira", "Areas Especiais"]),
    ("AC", "Rio Branco", "1200401", -9.9747, -67.8243,
     ["Centro", "Bosque", "Cidade Nova", "Estação Experimental"]),
    ("AC", "Cruzeiro do Sul", "1200500", -7.6275, -72.6717,
     ["Centro", "Miritizal", "Sao Salvador", "Cohab"]),
    ("RR", "Boa Vista", "1400100", 2.8235, -60.6758,
     ["Centro", "Sao Vicente", "Asa Branca", "Caçari"]),
    ("AP", "Macapa", "1600303", 0.0349, -51.0694,
     ["Centro", "Universidade", "Buritizal", "Nova Esperança"]),
    ("AP", "Santana", "1600600", -0.0583, -51.1814,
     ["Centro", "Vila Amazonas", "Provedor", "Igarapé da Fortaleza"]),
]


BANDEIRAS = [
    ("Petrobras", 1500), ("Shell", 1100), ("Ipiranga", 1300),
    ("Raizen", 800), ("Ale", 600), ("Brand-A", 200),
    ("Brand-B", 200), ("Bandeira Branca", 1500),
]

COMBUSTIVEIS = [
    ("GASOLINA_COMUM", 5.79, 0.40),
    ("GASOLINA_ADITIVADA", 5.95, 0.45),
    ("ETANOL", 4.39, 0.50),
    ("DIESEL_S10", 6.29, 0.35),
    ("DIESEL_S500", 6.05, 0.35),
    ("GNV", 4.59, 0.30),
]

LOGRADOUROS = [
    "Av. Brasil", "Av. Paulista", "Rua das Flores", "Av. Atlantica",
    "Rua XV de Novembro", "Av. Beira Mar", "Av. das Americas",
    "Rua Sao Joao", "Av. dos Bandeirantes", "Rua dos Andradas",
    "Av. Presidente Vargas", "Rua Sete de Setembro", "Av. JK",
    "Rua Marechal Deodoro", "Av. Senador Salgado Filho",
]

INTERACAO_TIPOS = [
    ("avaliacao", 0.55),
    ("compartilhamento", 0.20),
    ("check_in", 0.20),
    ("comentario", 0.05),
]


# ===========================================================================
# Helpers
# ===========================================================================
def _weighted_choice(pairs):
    pop, weights = zip(*pairs)
    return random.choices(pop, weights=weights, k=1)[0]


def _coord_around(lat: float, lon: float, max_km: float = 8.0) -> Tuple[float, float]:
    """Coordenada em torno do centro da cidade, em km. 1 grau ~= 111km."""
    rlat = max_km / 111.0
    rlon = rlat / max(math.cos(math.radians(lat)), 0.1)
    return (
        lat + random.uniform(-rlat, rlat),
        lon + random.uniform(-rlon, rlon),
    )


def _gen_cnpj() -> str:
    raw = [random.randint(0, 9) for _ in range(14)]
    s = "".join(str(d) for d in raw)
    return f"{s[0:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:14]}"


def _gen_cep() -> str:
    return f"{random.randint(10000, 99999)}{random.randint(100, 999)}"


def _now_minus(days: int) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(
        days=days, hours=random.randint(0, 23), minutes=random.randint(0, 59)
    )


# ===========================================================================
# Generators
# ===========================================================================
def gen_postos(n: int) -> List[dict]:
    """Distribui postos por cidade proporcionalmente ao numero de bairros."""
    weights = [len(c[5]) for c in CIDADES]
    indices = random.choices(range(len(CIDADES)), weights=weights, k=n)
    out = []
    for idx in indices:
        uf, cidade, ibge, lat0, lon0, bairros = CIDADES[idx]
        bairro = random.choice(bairros)
        lat, lon = _coord_around(lat0, lon0)
        bandeira = _weighted_choice(BANDEIRAS)
        nome = f"Posto {random.choice(['Sao', 'Boa', 'Santa', 'Bom'])} " \
               f"{random.choice(['Vista', 'Esperanca', 'Sucesso', 'Caminho'])} " \
               f"{random.choice(['Ltda', 'ME', 'EIRELI'])}"
        out.append({
            "_id": ObjectId(),
            "cnpj": _gen_cnpj(),
            "nome_fantasia": nome,
            "bandeira": bandeira,
            "endereco": {
                "logradouro": random.choice(LOGRADOUROS),
                "numero": str(random.randint(1, 9999)),
                "bairro": bairro,
                "cep": _gen_cep(),
                "cidade": cidade,
                "estado": uf,
            },
            "telefone": f"({random.randint(11, 99)}) {random.randint(2000,9999)}-{random.randint(1000,9999)}",
            "ativo": random.random() > 0.05,
            "location": {"type": "Point", "coordinates": [lon, lat]},
            "created_at": _now_minus(random.randint(60, 1500)),
            "updated_at": _now_minus(random.randint(0, 30)),
            # campo auxiliar (NAO salvo): so para correlacionar com as outras colecoes
            "_aux": {"uf": uf, "cidade": cidade, "ibge": ibge, "bairro": bairro,
                     "lat": lat, "lon": lon},
        })
    return out


def gen_localizacoes(postos: List[dict]) -> List[dict]:
    out = []
    for p in postos:
        aux = p["_aux"]
        out.append({
            "_id": ObjectId(),
            "posto_id": p["_id"],
            "municipio": aux["cidade"],
            "bairro": aux["bairro"],
            "uf": aux["uf"],
            "codigo_ibge": aux["ibge"],
            "geo": {"type": "Point", "coordinates": [aux["lon"], aux["lat"]]},
            "atualizado_em": _now_minus(random.randint(0, 60)),
        })
    return out


def gen_eventos_preco(postos: List[dict], n: int) -> List[dict]:
    """Cada posto tem em media n/len(postos) eventos."""
    out = []
    for _ in range(n):
        p = random.choice(postos)
        comb_tup = random.choice(COMBUSTIVEIS)
        comb, base, spread = comb_tup
        preco_anterior = round(base + random.uniform(-spread, spread), 3)
        # Variacao realista entre -10% e +10%
        delta = random.uniform(-0.10, 0.10)
        preco_novo = round(preco_anterior * (1 + delta), 3)
        variacao_pct = round(((preco_novo - preco_anterior) / preco_anterior) * 100, 4)
        out.append({
            "_id": ObjectId(),
            "posto_id": p["_id"],
            "combustivel": comb,
            "preco_anterior": preco_anterior,
            "preco_novo": preco_novo,
            "variacao_pct": variacao_pct,
            "unidade": "BRL_L",
            "fonte": random.choice(["crawler", "manual", "api"]),
            "ocorrido_em": _now_minus(random.randint(0, 30)),
            "revisado": random.random() > 0.2,
        })
    return out


def gen_buscas(postos: List[dict], n: int) -> List[dict]:
    """As buscas sao ancoradas nas cidades reais para que os filtros do
    dashboard tenham retorno efetivo."""
    out = []
    cidade_pool = list({(p["_aux"]["uf"], p["_aux"]["cidade"]) for p in postos})
    bairros_por_cidade = {}
    for c in CIDADES:
        bairros_por_cidade[(c[0], c[1])] = c[5]
    coords_por_cidade = {(c[0], c[1]): (c[3], c[4]) for c in CIDADES}

    for _ in range(n):
        uf, cidade = random.choice(cidade_pool)
        bairro = random.choice(bairros_por_cidade[(uf, cidade)])
        lat0, lon0 = coords_por_cidade[(uf, cidade)]
        lat, lon = _coord_around(lat0, lon0, max_km=10)
        comb = _weighted_choice([
            ("GASOLINA_COMUM", 50), ("GASOLINA_ADITIVADA", 15),
            ("ETANOL", 20), ("DIESEL_S10", 10), ("DIESEL_S500", 3), ("GNV", 2),
        ])
        out.append({
            "_id": ObjectId(),
            "usuario_id": str(uuid.uuid4()),
            "session_id": str(uuid.uuid4()),
            "tipo_combustivel": comb,
            "cidade": cidade,
            "estado": uf,
            "raio_km": random.choice([3, 5, 5, 5, 10, 10, 25]),
            "filtros": {
                "apenas_abertos": random.random() > 0.5,
                "ordenacao": random.choice(["preco", "distancia", "avaliacao"]),
                "bairro": bairro,
            },
            "geo_centro": {"type": "Point", "coordinates": [lon, lat]},
            "consultado_em": _now_minus(random.randint(0, 30)),
            "resultado_count": random.randint(0, 80),
            "latencia_ms": random.randint(40, 380),
        })
    return out


def gen_avaliacoes(postos: List[dict], n: int) -> List[dict]:
    out = []
    for _ in range(n):
        p = random.choice(postos)
        tipo = _weighted_choice(INTERACAO_TIPOS)
        nota = None
        comentario = None
        util = 0
        if tipo == "avaliacao":
            nota = random.choices([1, 2, 3, 4, 5], weights=[2, 3, 10, 35, 50])[0]
            util = random.randint(0, 25)
        elif tipo == "comentario":
            comentario = random.choice([
                "Atendimento otimo", "Preco bom", "Posto sempre limpo",
                "Demorou pra atender", "Recomendo o cafe da loja",
            ])
            util = random.randint(0, 5)
        elif tipo == "check_in":
            util = random.randint(0, 3)
        out.append({
            "_id": ObjectId(),
            "posto_id": p["_id"],
            "usuario_id": str(uuid.uuid4()),
            "tipo": tipo,
            "nota": nota,
            "comentario": comentario,
            "created_at": _now_minus(random.randint(0, 60)),
            "util_count": util,
        })
    return out


# ===========================================================================
# Insert helpers
# ===========================================================================
def _strip_aux(docs: List[dict]) -> List[dict]:
    """Remove campos auxiliares (_aux) antes de inserir no Mongo."""
    out = []
    for d in docs:
        c = dict(d)
        c.pop("_aux", None)
        out.append(c)
    return out


def _bulk_insert(col, docs: List[dict], batch: int = 1000) -> int:
    n = 0
    started = time.time()
    for i in range(0, len(docs), batch):
        col.insert_many(docs[i : i + batch], ordered=False)
        n += min(batch, len(docs) - i)
        if n % 10000 == 0 or n == len(docs):
            rate = n / max(time.time() - started, 1)
            log.info("  %s: %d/%d (%.0f docs/s)", col.name, n, len(docs), rate)
    return n


# ===========================================================================
# Main
# ===========================================================================
def main():
    parser = argparse.ArgumentParser()
    # Defaults somam 500.000 documentos (100k por colecao):
    #   100k postos + 100k localizacoes + 100k eventos + 100k buscas + 100k aval.
    parser.add_argument("--postos", type=int, default=100_000)
    parser.add_argument("--eventos", type=int, default=100_000)
    parser.add_argument("--buscas", type=int, default=100_000)
    parser.add_argument("--avaliacoes", type=int, default=100_000)
    parser.add_argument("--drop", action="store_true",
                        help="apaga as colecoes antes de popular")
    args = parser.parse_args()

    log.info("Conectando: %s / %s", MONGO_URI, MONGO_DB)
    log.info("Alvo: %d postos, %d eventos, %d buscas, %d avaliacoes (SEED=%d)",
             args.postos, args.eventos, args.buscas, args.avaliacoes, SEED)

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    if args.drop:
        log.info("Drop das colecoes...")
        for col in (COL_POSTOS, COL_LOCALIZACOES, COL_PRECOS, COL_BUSCAS, COL_AVALIACOES):
            db[col].drop()

    log.info("Gerando postos (%d)...", args.postos)
    postos = gen_postos(args.postos)
    _bulk_insert(db[COL_POSTOS], _strip_aux(postos))

    log.info("Gerando localizacoes_postos (%d)...", len(postos))
    locs = gen_localizacoes(postos)
    _bulk_insert(db[COL_LOCALIZACOES], locs)

    log.info("Gerando eventos_preco (%d)...", args.eventos)
    eventos = gen_eventos_preco(postos, args.eventos)
    _bulk_insert(db[COL_PRECOS], eventos)

    log.info("Gerando buscas_usuarios (%d)...", args.buscas)
    buscas = gen_buscas(postos, args.buscas)
    _bulk_insert(db[COL_BUSCAS], buscas)

    log.info("Gerando avaliacoes_interacoes (%d)...", args.avaliacoes)
    aval = gen_avaliacoes(postos, args.avaliacoes)
    _bulk_insert(db[COL_AVALIACOES], aval)

    # Indices de leitura recorrentes (opcional, nao obriga para o pipeline)
    db[COL_POSTOS].create_index([("location", "2dsphere")])
    db[COL_LOCALIZACOES].create_index([("geo", "2dsphere")])
    db[COL_PRECOS].create_index([("posto_id", 1), ("ocorrido_em", -1)])
    db[COL_BUSCAS].create_index([("estado", 1), ("cidade", 1)])
    db[COL_AVALIACOES].create_index([("posto_id", 1)])

    log.info("Concluido. Total: %d documentos.",
             args.postos * 2 + args.eventos + args.buscas + args.avaliacoes)


if __name__ == "__main__":
    main()
