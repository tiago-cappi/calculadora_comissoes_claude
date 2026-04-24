"""
parse_fat_rent_gpe.py
Extrai Part Numbers (codigo_produto) e rentabilidade realizada (%) do arquivo
fat_rent_gpe.csv exportado pelo ERP da Clean Environment Brasil.

Cada linha do CSV é um registro completo do relatório (com cabeçalho repetido).
O script ignora totais de grupo e extrai apenas itens individuais.

Saída: DataFrame / CSV com colunas:
    codigo_produto   str    Código do produto (ex: CEE201266)
    venda_liq        float  Venda Líquida do item (R$)
    rentabilidade    float  % Rentabilidade realizada (ex: 30.84)
"""

import csv
import re
import io
from dataclasses import dataclass, field
from typing import Optional, Union


# ---------------------------------------------------------------------------
# Constantes de posição dos campos no CSV (0-indexed)
# ---------------------------------------------------------------------------
FIELD_PRODUTO = 13   # "CEE201266 - BUCHA DA BALANÇA PARA AP3"
FIELD_DADOS   = 14   # "    4     1.157,66  803,59  555,77 ... 247,82  30,84"

# Regex para capturar todos os números da linha de dados (Qtde, valores, %)
RE_NUMERO = re.compile(r'-?\d{1,3}(?:\.\d{3})*(?:,\d+)?')


def _parse_br_float(s: str) -> float:
    """Converte string numérica no formato BR ('1.234,56') para float."""
    return float(s.replace('.', '').replace(',', '.'))


def _extract_numbers(texto: str) -> list[float]:
    """Extrai todos os números de uma string de dados do relatório."""
    return [_parse_br_float(m) for m in RE_NUMERO.findall(texto)]


@dataclass
class ParseResult:
    """Resultado do parsing do fat_rent_gpe.csv."""
    items: list[dict] = field(default_factory=list)   # lista de {codigo, venda_liq, rentabilidade}
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0 and len(self.items) > 0

    def to_dataframe(self):
        import pandas as pd
        return pd.DataFrame(self.items)

    def summary(self) -> str:
        lines = [
            f"✓ {len(self.items)} Part Numbers extraídos",
        ]
        if self.warnings:
            lines += [f"⚠ {w}" for w in self.warnings]
        if self.errors:
            lines += [f"✖ {e}" for e in self.errors]
        return "\n".join(lines)


def execute(
    source: Union[str, bytes, io.IOBase],
    encoding: str = "latin-1",
) -> ParseResult:
    """
    Processa o arquivo fat_rent_gpe.csv e retorna os Part Numbers com rentabilidade.

    Parâmetros
    ----------
    source   : caminho para o arquivo (str), bytes do arquivo, ou file-like object
    encoding : encoding do arquivo (padrão: latin-1)

    Retorna
    -------
    ParseResult com .items (list of dicts), .ok, .summary()
    """
    result = ParseResult()

    # --- Carregamento ---
    if isinstance(source, (str,)):
        with open(source, encoding=encoding) as f:
            raw_lines = f.readlines()
    elif isinstance(source, bytes):
        raw_lines = source.decode(encoding).splitlines(keepends=True)
    else:
        raw_lines = source.read().decode(encoding).splitlines(keepends=True)

    seen_codes = {}  # codigo_produto -> index em result.items (para deduplicação somada)

    for line_num, raw_line in enumerate(raw_lines, start=1):
        # Cada linha é um CSV completo — parsear com csv.reader
        try:
            fields = next(csv.reader([raw_line]))
        except Exception as e:
            result.warnings.append(f"Linha {line_num}: erro ao parsear CSV — {e}")
            continue

        if len(fields) <= FIELD_DADOS:
            result.warnings.append(f"Linha {line_num}: campos insuficientes ({len(fields)}), ignorada.")
            continue

        produto_raw = fields[FIELD_PRODUTO].strip()
        dados_raw   = fields[FIELD_DADOS].strip()

        # Ignora linhas sem produto ou linhas de totais de grupo / rodapé
        if not produto_raw:
            continue
        if produto_raw.startswith("TOTAL") or produto_raw.startswith("TOTAIS"):
            continue

        # Extrai código do produto (tudo antes do primeiro " - ")
        partes = produto_raw.split(" - ", 1)
        codigo = partes[0].strip()
        if not codigo:
            result.warnings.append(f"Linha {line_num}: código vazio em '{produto_raw}', ignorado.")
            continue

        # Extrai os números da linha de dados
        numeros = _extract_numbers(dados_raw)
        # Esperamos: [Qtde, Total_Venda, Venda_Liq, Custo_Liq_I, ICMS_ST, DIFAL, Custo_Liq_II, Valor_Rentab, Pct_Rentab]
        # Índice    :   0        1           2            3           4       5        6              7              8
        if len(numeros) < 9:
            result.warnings.append(
                f"Linha {line_num}: '{codigo}' — números insuficientes ({len(numeros)}), ignorado."
            )
            continue

        venda_liq     = numeros[2]   # Venda Líquida
        rentabilidade = numeros[8]   # % Rentabilidade

        # Deduplicação: mesmo código pode aparecer em linhas separadas — soma venda_liq, recalcula % ponderada
        if codigo in seen_codes:
            idx = seen_codes[codigo]
            existing = result.items[idx]
            # Média ponderada acumulada: recalcula usando valores absolutos
            # Rentab_valor = Valor_Rentab (numeros[7]) — necessário para ponderar corretamente
            valor_rentab_novo     = numeros[7]
            valor_rentab_existing = existing["_valor_rentab"]

            nova_venda_liq = existing["venda_liq"] + venda_liq
            novo_valor_rentab = valor_rentab_existing + valor_rentab_novo

            if nova_venda_liq != 0:
                nova_pct = (novo_valor_rentab / nova_venda_liq) * 100
            else:
                nova_pct = 0.0

            existing["venda_liq"]      = nova_venda_liq
            existing["rentabilidade"]  = round(nova_pct, 4)
            existing["_valor_rentab"]  = novo_valor_rentab
        else:
            seen_codes[codigo] = len(result.items)
            result.items.append({
                "codigo_produto": codigo,
                "venda_liq":      venda_liq,
                "rentabilidade":  rentabilidade,
                "_valor_rentab":  numeros[7],  # campo interno para deduplicação
            })

    # Remove campo interno antes de retornar
    for item in result.items:
        item.pop("_valor_rentab", None)

    if not result.items:
        result.errors.append("Nenhum Part Number encontrado no arquivo.")

    return result


# ---------------------------------------------------------------------------
# CLI simples para uso direto
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    import pandas as pd

    if len(sys.argv) < 2:
        print("Uso: python parse_fat_rent_gpe.py <caminho_arquivo.csv> [saida.csv]")
        sys.exit(1)

    res = execute(sys.argv[1])
    print(res.summary())

    if res.ok:
        df = res.to_dataframe()
        out_path = sys.argv[2] if len(sys.argv) > 2 else "fat_rent_parsed.csv"
        df.to_csv(out_path, index=False, sep=";", decimal=",", encoding="utf-8-sig")
        print(f"\nSaída gravada em: {out_path}")
        from scripts.terminal_display import print_df
        print_df(df.head(10))
