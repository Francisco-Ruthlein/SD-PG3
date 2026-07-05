# Servicio de Descubrimiento P2P

Proyecto base para la Practica Guiada 3: Gossip + DHT Chord simplificado.

## Objetivo

Nodo que ejecuta Gossip (membresía P2P) + DHT Chord (lookup distribuido) simultáneamente.

## Integrantes

Ruthlein Francisco


## Ejecucion

### Local (un nodo)

```bash
# Nodo seed
NODO_ID=1 HTTP_PORT=8080 RPC_PORT=5000 go run ./cmd/nodo

# Nodo que se une al seed
NODO_ID=50 HTTP_PORT=8081 RPC_PORT=5001 SEED=localhost:5000 go run ./cmd/nodo
```

### Docker Compose (5 nodos)

```bash
make build
make run
make status
make logs
make stop
```

## Pruebas manuales

### Ver estado
```bash
curl http://localhost:8081/estado
```

### Almacenar un valor en el DHT
```bash
# clave 1: responsable = nodo 1 (rango (255, 1])
curl -X POST http://localhost:8081/almacenar \
  -H "Content-Type: application/json" \
  -d '{"clave":1,"valor":"hola"}'
```

### Buscar un valor en el DHT
```bash
curl "http://localhost:8081/buscar?clave=1"
```

### Probar lookup cross-nodo (forwarding por finger table)

```bash
# clave 25: responsable = nodo 50 (rango (1, 50])
# Pedir al nodo 1 que almacene: forwardeará al nodo 50 por RPC.
curl -X POST http://localhost:8080/almacenar \
  -H "Content-Type: application/json" \
  -d '{"clave":25,"valor":"mundo"}'

# Buscar desde el nodo 1: forwardeará al nodo 50.
curl "http://localhost:8080/buscar?clave=25"
```

### Verificar estabilización del anillo Chord

```bash
# Arrancar un tercer nodo (NODO_ID=100) sin añadirlo a PEERS de los otros
# dos, solo vía SEED. Gossip lo descubrirá; en hasta 10 s el bucle de
# estabilización Chord lo integrará al anillo.
NODO_ID=100 HTTP_PORT=8082 RPC_PORT=5002 SEED=localhost:5000 go run ./cmd/nodo

# Tras ~10 s, el /estado del nodo 1 debería mostrar finger_table
# apuntando al nuevo sucesor/predecesor del anillo completo.
curl http://localhost:8080/estado
```

## Requisitos completados

- [ ] TODO 1-7: Gossip - membresia, anti-entropia (`pkg/gossip/nodo.go`)
- [ ] TODO 8-14: DHT - finger table, lookup (`pkg/dht/chord.go`)
- [ ] TODO 15-20: Nodo HTTP + RPC (`cmd/nodo/main.go`)
- [ ] Docker Compose con 5 nodos

