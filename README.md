# FilmeBox - Algoritmo de Previsão de Bilheteria de Filmes

## Visão Geral
 FilmeBox é um projeto desenvolvido para prever a bilheteria de filmes utilizando um algoritmo de machine learning. Este projeto é flexível o suficiente para receber diferentes conjuntos de dados CSV, combiná-los e, em seguida, consultar a API do TMDB para obter dados adicionais sobre os filmes. Esses dados são então utilizados para criar a base final de dados sobre a qual o algoritmo é aplicado.

## Funcionalidades Principais
- Importação de conjuntos de dados CSV contendo informações sobre filmes.
- Consulta à API do TMDB para obter informações adicionais sobre os filmes.
- Combinação de dados importados e dados da API para formar uma base de dados completa.
- Aplicação de algoritmo de previsão de bilheteria para fornecer estimativas de bilheteria para os filmes.

## Pré-requisitos
- Python 3.12.2
- Bibliotecas:
    * Pandas: `2.2.1`
    * Requests: `2.31.0`

## Instalação/Uso

1. Clone o repositório do FilmeBox para o seu ambiente local: 
 ```bash
    git clone https://github.com/JpedroCSantos/tcc_boxOffice.git
```
2. Definir a versao do Python usando o `pyenv local 3.12.1`
2. `poetry env use 3.12.1`, `poetry install --no-root` e `poetry lock --no-update`.
3. Criar uma [chave de API do TMDB](https://developer.themoviedb.org/reference/intro/getting-started).
4. Execute o comando `python app/database.py` para gerar o arquivo csv da base de dados.
5. Certifique-se de instalar as versões especificadas das bibliotecas Pandas, Requests
6. Execute os scripts `python src/main.py` através de um terminal ou ambiente de desenvolvimento que suporte Python.


## Contribuição
Contribuições são bem-vindas! Se você deseja contribuir para o FilmeBox, siga estas etapas:

1. Fork o projeto.
2. Crie uma branch para sua feature (git checkout -b feature/NomeDaFeature).
3. Commit suas mudanças (git commit -am 'Adicionando uma nova feature').
4. Push para a branch (git push origin feature/NomeDaFeature).
5. Crie um novo Pull Request.

## Autores
* [João Pedro Santos](https://www.linkedin.com/in/jpedro-santos/)