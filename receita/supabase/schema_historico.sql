create schema if not exists comissoes;

-- Fonte unificada da verdade para recebimento.
-- Uma linha por GL + processo + documento + tipo_pagamento + periodo.
create table if not exists comissoes.historico_comissoes (
    nome text not null,
    cargo text not null default '',
    processo text not null,
    numero_pc text not null default '',
    codigo_cliente text not null default '',
    tipo text not null default 'recebimento',
    tipo_pagamento text not null default 'REGULAR',
    documento text not null default '',
    nf_extraida text not null default '',
    linha_negocio text not null default '',
    status_processo text not null default '',
    mes_apuracao integer not null check (mes_apuracao between 1 and 12),
    ano_apuracao integer not null check (ano_apuracao between 2000 and 2100),
    valor_documento numeric(18, 2) not null default 0,
    valor_processo numeric(18, 2) not null default 0,
    tcmp double precision not null default 0,
    fcmp_rampa double precision not null default 1,
    fcmp_aplicado double precision not null default 1,
    fcmp_considerado double precision not null default 1,
    fcmp_modo text not null default 'PROVISORIO',
    comissao_potencial numeric(18, 4) not null default 0,
    comissao_adiantada numeric(18, 4) not null default 0,
    comissao_total numeric(18, 4) not null default 0,
    status_faturamento_completo boolean not null default false,
    status_pagamento_completo boolean null,
    reconciliado boolean not null default false,
    ac_snapshot_json text not null default '[]',
    af_snapshot_json text not null default '[]',
    tcmp_detalhes_json text not null default '[]',
    fcmp_detalhes_json text not null default '[]',
    created_at timestamptz not null default now(),
    constraint historico_comissoes_pk
        primary key (nome, processo, documento, tipo_pagamento, mes_apuracao, ano_apuracao)
);

alter table comissoes.historico_comissoes
    add column if not exists cargo text not null default '',
    add column if not exists numero_pc text not null default '',
    add column if not exists codigo_cliente text not null default '',
    add column if not exists tipo_pagamento text not null default 'REGULAR',
    add column if not exists documento text not null default '',
    add column if not exists nf_extraida text not null default '',
    add column if not exists status_processo text not null default '',
    add column if not exists valor_documento numeric(18, 2) not null default 0,
    add column if not exists tcmp double precision not null default 0,
    add column if not exists fcmp_rampa double precision not null default 1,
    add column if not exists fcmp_aplicado double precision not null default 1,
    add column if not exists fcmp_considerado double precision not null default 1,
    add column if not exists fcmp_modo text not null default 'PROVISORIO',
    add column if not exists comissao_potencial numeric(18, 4) not null default 0,
    add column if not exists comissao_adiantada numeric(18, 4) not null default 0,
    add column if not exists status_faturamento_completo boolean not null default false,
    add column if not exists status_pagamento_completo boolean null,
    add column if not exists reconciliado boolean not null default false,
    add column if not exists ac_snapshot_json text not null default '[]',
    add column if not exists af_snapshot_json text not null default '[]',
    add column if not exists tcmp_detalhes_json text not null default '[]',
    add column if not exists fcmp_detalhes_json text not null default '[]';

-- Limpeza: remove coluna fator_split de bancos ja migrados (logica descontinuada).
alter table comissoes.historico_comissoes
    drop column if exists fator_split;

alter table comissoes.historico_comissoes
    drop constraint if exists historico_comissoes_pk;

alter table comissoes.historico_comissoes
    add constraint historico_comissoes_pk
    primary key (nome, processo, documento, tipo_pagamento, mes_apuracao, ano_apuracao);

create index if not exists historico_comissoes_parent_idx
    on comissoes.historico_comissoes (numero_pc, codigo_cliente, reconciliado);

create index if not exists historico_comissoes_periodo_idx
    on comissoes.historico_comissoes (ano_apuracao, mes_apuracao);

create index if not exists historico_comissoes_processo_idx
    on comissoes.historico_comissoes (processo, nome, documento);

-- Historico mensal do vinculo entre Processo Pai e seus processos.
create table if not exists comissoes.historico_processo_pai (
    numero_pc text not null,
    codigo_cliente text not null,
    processo text not null,
    is_processo_pai boolean not null default false,
    status_faturado boolean not null default false,
    status_pago boolean null,
    mes_referencia integer not null check (mes_referencia between 1 and 12),
    ano_referencia integer not null check (ano_referencia between 2000 and 2100),
    created_at timestamptz not null default now(),
    constraint historico_processo_pai_pk
        primary key (numero_pc, codigo_cliente, processo, mes_referencia, ano_referencia)
);

create index if not exists historico_processo_pai_periodo_idx
    on comissoes.historico_processo_pai (ano_referencia, mes_referencia);

-- Historico mensal das parcelas/documentos da AF ligados ao Processo Pai.
create table if not exists comissoes.historico_pagamentos_processo_pai (
    numero_pc text not null,
    codigo_cliente text not null,
    processo text not null,
    numero_nf text not null default '',
    documento text not null,
    situacao_codigo integer not null default -1,
    situacao_texto text not null default 'Desconhecido',
    dt_prorrogacao timestamptz null,
    data_baixa timestamptz null,
    valor_documento numeric(18, 2) not null default 0,
    mes_referencia integer not null check (mes_referencia between 1 and 12),
    ano_referencia integer not null check (ano_referencia between 2000 and 2100),
    created_at timestamptz not null default now(),
    constraint historico_pagamentos_processo_pai_pk
        primary key (numero_pc, codigo_cliente, documento, mes_referencia, ano_referencia)
);

create index if not exists historico_pagamentos_processo_pai_periodo_idx
    on comissoes.historico_pagamentos_processo_pai (ano_referencia, mes_referencia);
