# Dicionário de Dados

Descrição detalhada dos campos de cada fonte de dados disponível no diretório `data/`.

---

## veiculos.csv

Cadastro da frota de veículos da empresa.

| Campo | Tipo | Descrição |
|---|---|---|
| `veiculo_id` | string | Identificador único do veículo (ex: `VEI-0001`) |
| `placa` | string | Placa do veículo (formato Mercosul) |
| `marca` | string | Fabricante (ex: `Volvo`, `Scania`, `Mercedes-Benz`) |
| `modelo` | string | Modelo do veículo |
| `ano_fabricacao` | int | Ano de fabricação |
| `tipo` | string | Tipo do veículo: `VUC`, `Caminhão Toco`, `Caminhão Truck`, `Carreta Simples`, `Carreta LS`, `Bitrem` |
| `capacidade_kg` | int | Capacidade máxima de carga em quilogramas |
| `capacidade_paletes` | int | Capacidade em número de paletes |
| `km_atual` | int | Quilometragem atual do veículo |
| `status` | string | Status: `ativo`, `em_manutencao`, `inativo` |
| `data_ultima_revisao` | date | Data da última revisão mecânica |

---

## motoristas.json

Cadastro de motoristas da empresa.

| Campo | Tipo | Descrição |
|---|---|---|
| `motorista_id` | string | Identificador único do motorista (ex: `MOT-0001`) |
| `nome` | string | Nome completo |
| `cpf` | string | CPF do motorista |
| `cnh` | string | Número da CNH |
| `categoria_cnh` | string | Categoria da CNH: `C`, `D`, `E` |
| `validade_cnh` | date | Data de validade da CNH |
| `telefone` | string | Telefone de contato |
| `data_admissao` | date | Data de admissão na empresa |
| `base_operacional` | string | Centro de distribuição base do motorista |
| `status` | string | Status: `ativo`, `ferias`, `afastado`, `desligado` |

---

## geocercas.geojson

Cercas geoespaciais definidas como polígonos no padrão GeoJSON (FeatureCollection). Cada Feature contém:

### Properties

| Campo | Tipo | Descrição |
|---|---|---|
| `geocerca_id` | string | Identificador único da geocerca (ex: `GEO-0001`) |
| `nome` | string | Nome descritivo do local |
| `tipo` | string | Tipo: `centro_distribuicao`, `pedagio`, `posto_combustivel`, `cliente` |
| `uf` | string | UF do estado (quando aplicável) |
| `raio_km` | float | Raio aproximado da geocerca em km |
| `ativo` | boolean | Se a geocerca está ativa |

### Geometry

| Campo | Tipo | Descrição |
|---|---|---|
| `type` | string | Sempre `Polygon` |
| `coordinates` | array | Array de coordenadas `[longitude, latitude]` formando o polígono |

---

## viagens.csv

Registro de viagens realizadas pela frota.

| Campo | Tipo | Descrição |
|---|---|---|
| `viagem_id` | string | Identificador único da viagem (ex: `VIA-000001`) |
| `veiculo_id` | string | FK → `veiculos.veiculo_id` |
| `motorista_id` | string | FK → `motoristas.motorista_id` |
| `geocerca_origem_id` | string | FK → `geocercas.geocerca_id` — ponto de partida |
| `geocerca_destino_id` | string | FK → `geocercas.geocerca_id` — ponto de chegada |
| `data_inicio` | datetime | Data/hora de início da viagem |
| `data_fim_prevista` | datetime | Data/hora prevista de chegada |
| `data_fim_real` | datetime | Data/hora real de chegada (nulo se não concluída) |
| `status` | string | Status: `em_transito`, `concluida`, `cancelada`, `atrasada` |
| `distancia_km` | int | Distância estimada da rota em km |
| `peso_carga_kg` | int | Peso da carga transportada em kg |
| `nota_fiscal` | string | Número da nota fiscal da carga |

---

## posicoes.parquet (rastreamento)

Posições GPS coletadas dos veículos durante as viagens. Cada registro representa **um ponto de telemetria**.

| Campo | Tipo | Descrição |
|---|---|---|
| `posicao_id` | string | Identificador único da posição (ex: `POS-A1B2C3D4E5`) |
| `viagem_id` | string | FK → `viagens.viagem_id` |
| `veiculo_id` | string | FK → `veiculos.veiculo_id` |
| `latitude` | float | Latitude da posição (WGS 84) |
| `longitude` | float | Longitude da posição (WGS 84) |
| `timestamp` | datetime | Data/hora da coleta do ponto |
| `velocidade_kmh` | int | Velocidade instantânea em km/h |
| `ignicao` | boolean | Se a ignição do veículo estava ligada |
| `odometro_metros` | float | Leitura do odômetro em metros |

---

## Relacionamentos

```
motoristas (1) ──── (N) viagens
veiculos   (1) ──── (N) viagens
geocercas  (1) ──── (N) viagens (origem)
geocercas  (1) ──── (N) viagens (destino)
viagens    (1) ──── (N) posicoes (rastreamento)
veiculos   (1) ──── (N) posicoes (rastreamento)
```

### Relacionamento espacial (geoespacial)

```
posicoes  ──[point-in-polygon]──  geocercas
```

Cada posição GPS pode estar dentro de zero ou mais geocercas. A detecção de qual geocerca o veículo se encontra deve ser feita por cálculo geoespacial (point-in-polygon).

---

> **Nota:** existem inconsistências propositais nos dados (duplicatas, valores nulos, coordenadas zeradas, registros órfãos, velocidades absurdas, CPFs inválidos). O tratamento correto dessas inconsistências é parte da avaliação.
