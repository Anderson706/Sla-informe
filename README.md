# App Flask - Comparativo de Códigos Bipados

## O que este projeto faz
- importa 2 planilhas Excel
- salva os arquivos no banco interno SQLite
- lê o código bipado ou digitado
- compara o código nas duas bases
- pega a primeira entrada da Base A
- pega os horários processados da Base B
- calcula deltas em minutos
- salva o histórico de comparativos no banco
- exporta histórico e comparativo em Excel

## Estrutura esperada das planilhas
O sistema tenta detectar as colunas automaticamente.

Sugestão:
- Base A: coluna de código + coluna de entrada
- Base B: coluna de código + coluna de processamento

Você também pode informar manualmente os nomes das colunas no formulário.

## Instalação
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

## Rotas principais
- `/` -> tela inicial
- `/upload` -> upload das duas planilhas
- `/scan` -> processa o código bipado
- `/export/bipes.xlsx` -> exporta histórico de bipes
- `/export/comparativo.xlsx` -> exporta comparativo

## Observações
- o banco é `app.db`
- os arquivos originais ficam em `instance_uploads/`
- a câmera usa a biblioteca `html5-qrcode`
- também funciona com leitor USB no campo de texto
