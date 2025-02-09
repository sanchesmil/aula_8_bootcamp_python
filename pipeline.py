from etl import pipeline_calcular_kpi_vendas_consolidada

pasta = 'data'
formato_saida: list = ['csv','parquet']

# chama a função que executa todo o pipeline de leitura,agregacao,transformacao e carregamento
pipeline_calcular_kpi_vendas_consolidada(pasta,formato_saida)