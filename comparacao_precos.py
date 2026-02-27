"""Script de exemplo para gerar uma base de comparação anotada com preços.

O módulo `utils.loaders` agora oferece a função
`carregar_comparacao_master_com_precos` que resolve exatamente o que a
solicitação descreve: ele encontra o último arquivo `comparacao_categorias_MASTER`,
captura os cinco arquivos mais recentes de cada uma das quatro bases de
resultados (amazon_FULL, amazon_SAMPLE, ml_FULL, ml_SAMPLE), extrai o preço de
cada produto pela última ocorrência do nome e injeta duas colunas
(`Preço Prod Amazon` e `Preço Prod ML`) imediatamente antes dos links.

Execute este script na raiz do projeto para produzir um CSV anotado ou para
utilizar a função em outro código (por exemplo, num notebook).
"""

from utils.loaders import carregar_comparacao_master_com_precos


def main():
    df = carregar_comparacao_master_com_precos()
    if df.empty:
        print("Não foi possível encontrar a base de comparação MASTER ou as bases de preços.")
        return
    # mostrar primeiras linhas
    print(df.head())

    destino = "data/comparacoes/comparacao_categorias_MASTER_com_precos.csv"
    df.to_csv(destino, sep=";", index=False, encoding="utf-8-sig")
    print(f"Arquivo anotado salvo em: {destino}")


if __name__ == "__main__":
    main()
