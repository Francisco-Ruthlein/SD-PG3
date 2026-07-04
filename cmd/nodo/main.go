package main

import (
	"encoding/json"
	"fmt"
	"log"
	
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"sd-descubrimiento/pkg/dht"
	"sd-descubrimiento/pkg/gossip"
)

var (
	idNodo      string
	idNum       int
	puertoHTTP  string
	puertoRPC   string
	pares       map[int]string
	gossipNodo  *gossip.NodoGossip
	chordNodo   *dht.NodoChord
	miDireccion string
)

func main() {
	idNodo = os.Getenv("NODO_ID")
	if idNodo == "" {
		idNodo = "1"
	}
	puertoHTTP = os.Getenv("HTTP_PORT")
	if puertoHTTP == "" {
		puertoHTTP = "8080"
	}
	puertoRPC = os.Getenv("RPC_PORT")
	if puertoRPC == "" {
		puertoRPC = "5000"
	}

	pares = parsearPares(os.Getenv("PEERS"))
	idNum, _ = strconv.Atoi(idNodo)
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	miDireccion = fmt.Sprintf("%s:%s", hostname, puertoRPC)

		// Inicializar nodos
	gossipNodo = gossip.NuevoNodo(idNum, miDireccion)

	seed := os.Getenv("SEED")
	if seed != "" {
		// Resolver la Direccion canónica del seed via Identificarse para
		// evitar duplicados en la membresía. Si falla, usar el seed crudo.
		c, err := rpc.Dial("tcp", seed)
		if err == nil {
			var ri gossip.RespIdentificacion
			if c.Call("ServicioGossip.Identificarse", gossip.ArgsVacio{}, &ri) == nil && ri.Direccion != "" {
				gossipNodo.Unirse(ri.Direccion)
			} else {
				gossipNodo.Unirse(seed)
			}
			c.Close()
		} else {
			gossipNodo.Unirse(seed)
		}
	}

	// TODO 15: inicializar chordNodo con sucesor y predecesor calculados
	// a partir de NODO_ID y PEERS (IDs ordenados en el anillo 0-255).
	chordNodo = nil // STUB

	// Endpoints HTTP
	http.HandleFunc("/estado", manejadorEstado)
	http.HandleFunc("/almacenar", manejadorAlmacenar)
	http.HandleFunc("/buscar", manejadorBuscar)

	// Servicio RPC
	// TODO: registrar el servicio RPC para Gossip y DHT
	go iniciarRPC()

	// Loop anti-entropia
	go bucleAntiEntropia()

	// Loop estabilización del anillo Chord (cada 10 segundos)
	go bucleEstabilizacionChord()

	addr := ":" + puertoHTTP
	fmt.Printf("[NODO %s] Escuchando HTTP en %s, RPC en %s\n", idNodo, addr, puertoRPC)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// parsearPares parsea los valores pasados como argumentos desde el shell
func parsearPares(peersEnv string) map[int]string {
	resultado := make(map[int]string)
	if peersEnv == "" {
		return resultado
	}
	partes := strings.Split(peersEnv, ",")
	for _, p := range partes {
		kv := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(kv) != 2 {
			continue
		}
		id, err := strconv.Atoi(kv[0])
		if err != nil {
			continue
		}
		resultado[id] = kv[1]
	}
	return resultado
}

// TODO 16: Implementar iniciarRPC.
// Debe crear un listener TCP en puertoRPC, registrar un servicio RPC
// que maneje intercambios de Gossip y lookups de DHT, y atender conexiones.
func iniciarRPC() {
	// COMPLETAR
}

// TODO 17: Implementar bucleAntiEntropia.
// Cada 5 segundos, obtener un par con gossipNodo.AntiEntropia().
// Si hay par, conectarse via RPC, intercambiar miembros y fusionar.
func bucleAntiEntropia() {
	// COMPLETAR
}

// TODO 18: Implementar manejadorEstado.
// GET /estado devuelve JSON con node_id, miembros y finger_table.
func manejadorEstado(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":      idNodo,
		"miembros":     gossipNodo.ObtenerMiembros(),
		"finger_table": []string{}, // STUB
	})
}

// TODO 19: Implementar manejadorAlmacenar.
// POST /almacenar recibe JSON {"clave": int, "valor": string}.
// Si el nodo es responsable (chordNodo.EsResponsable), almacenar localmente.
// Si no, reenviar al sucesor via RPC.
func manejadorAlmacenar(w http.ResponseWriter, r *http.Request) {
	// STUB
	w.WriteHeader(http.StatusNotImplemented)
}

// TODO 20: Implementar manejadorBuscar.
// GET /buscar?clave=X
// Si responsable, devolver valor. Si no, reenviar o redirigir.
func manejadorBuscar(w http.ResponseWriter, r *http.Request) {
	// STUB
	w.WriteHeader(http.StatusNotImplemented)
}

// bucleEstabilizacionChord recalcula el anillo Chord cada 10 segundos.
// Consulta a todos los miembros descubiertos por Gossip via RPC
// (ServicioGossip.Identificarse) para obtener su ID lógico y Direccion
// canónica, luego reordena los IDs y recalcula sucesor/predecesor.
// Si un miembro deja de responder (crash), se excluye del recalculo y
// el anillo se auto-repara en ~10s. Si Gossip descubre un nuevo nodo,
// entra al anillo en la proxima estabilización.
func bucleEstabilizacionChord() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if chordNodo == nil {
			continue
		}
		miembros := gossipNodo.ObtenerMiembros()
		conocidos := map[int]string{idNum: miDireccion}
		for _, dir := range miembros {
			if dir == miDireccion {
				continue
			}
			c, err := rpc.Dial("tcp", dir)
			if err != nil {
				continue
			}
			var ri gossip.RespIdentificacion
			if c.Call("ServicioGossip.Identificarse", gossip.ArgsVacio{}, &ri) == nil {
				conocidos[ri.ID] = ri.Direccion
			}
			c.Close()
		}
		ids := []int{}
		for id := range conocidos {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		n := len(ids)
		miIdx := 0
		for i, v := range ids {
			if v == idNum {
				miIdx = i
				break
			}
		}
		predID := ids[(miIdx-1+n)%n]
		sucID := ids[(miIdx+1)%n]
		if n == 1 || predID == idNum {
			chordNodo.ActualizarAnillo(miDireccion, idNum, miDireccion, idNum)
		} else {
			chordNodo.ActualizarAnillo(conocidos[sucID], sucID, conocidos[predID], predID)
		}
		fmt.Printf("[NODO %s] Estabilización Chord: anillo=%v pred=%d suc=%d\n",
			idNodo, ids, predID, sucID)
	}
}
