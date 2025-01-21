import pandas as ps
import sys

print(sys.argv)

# o primeiro argumento é o nome do arquivo
# esse argumento será passado como parâmetro do conteiner ('docker run -it teste:pandas 2025-01-07')
day = sys.argv[1]

# transformações...

print(f"job concluído no dia: {day}")