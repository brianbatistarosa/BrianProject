# Brewery Project

Introdução:

Uma base de dados em formato JSON contendo informações sobre cervejarias nos Estados Unidos foi disponibilizada para estudo. O desenvolvimento foi realizado utilizando a arquitetura "medallion", visando explorar e analisar esses dados por meio do Databricks Community Edition


Arquitetura:

A arquitetura "medallion" foi empregada com o objetivo de organizar o lakehouse, estruturando os dados em camadas Bronze, Prata e Ouro. Esse método garantiu a qualidade das informações, permitindo seu tratamento adequado e gerando os insights necessários para uma análise abrangente.


Camada Bronze:

Na camada Bronze, foi realizada a extração das informações de cervejarias a partir de um JSON disponibilizado pela API "https://api.openbrewerydb.org/breweries". Os dados foram armazenados em "dbfs:/databricks/driver/bronze/" sem nenhum tratamento prévio, preservando a integridade da fonte original.


Camada Prata:

Na camada Prata, tratamos as informações de Latitude e Longitude, além de incluir uma chave hash baseada nos principais dados e a informação de timestamp para identificar o momento da extração.

Realizamos a validação de dados nulos e duplicados, garantindo a qualidade das informações. Para gerenciar possíveis erros, foi incluído um bloco try que gera um log tanto em caso de sucesso quanto de falha.

Os dados tratados foram salvos no diretório "dbfs:/databricks/driver/silver/".


Camada Ouro:

Após o tratamento realizado na camada anterior, a camada Ouro é responsável por disponibilizar as informações analíticas de maneira otimizada. Com os dados já processados, foi possível visualizar os locais, o porte das cervejarias e identificar a quantidade de unidades.

Os dados finais foram disponibilizados no diretório "dbfs:/databricks/driver/gold/".


Conclusão:

O estado com o maior número de cervejarias e de maior porte é o Colorado, seguido por Oregon e Califórnia, que possuem unidades de grande e pequeno porte. A Carolina do Norte e Ohio possuem duas unidades cada. Apenas dois estados registraram fechamento de unidades: Califórnia e Washington.

