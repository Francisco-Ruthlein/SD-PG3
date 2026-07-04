package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
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
	// Construir el anillo con todos los nodos conocidos (yo + PEERS)
todos := map[int]string{idNum: miDireccion}
for id, dir := range pares {
    todos[id] = dir
}
idsAnillo := []int{}
for id := range todos {
    idsAnillo = append(idsAnillo, id)
}
sort.Ints(idsAnillo)
nAnillo := len(idsAnillo)
miIdx := 0
for i, v := range idsAnillo {
    if v == idNum {
        miIdx = i
        break
    }
}
predID := idsAnillo[(miIdx-1+nAnillo)%nAnillo]
sucID := idsAnillo[(miIdx+1)%nAnillo]
sucDir := todos[sucID]
predDir := todos[predID]
if nAnillo == 1 {
    sucDir = miDireccion
    predDir = miDireccion
}
chordNodo = dht.NuevoNodo(idNum, miDireccion, sucDir, sucID, predDir, predID)

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
	// Creamos las instancias de los servicios RPC
	sg := &gossip.ServicioGossip{Nodo: gossipNodo}
	sc := &dht.ServicioChord{Nodo: chordNodo}
	// Registramos ambos servicios en el servidor RPC de Go
	if err := rpc.Register(sg); err != nil {
		log.Fatalf("Error al registrar el servicio de Gossip RPC: %v", err)
	}
	if err := rpc.Register(sc); err != nil {
		log.Fatalf("Error al registrar el servicio de Chord RPC: %v", err)
	}
	// Escuchamos conexiones TCP en el puerto RPC indicado
	listener, err := net.Listen("tcp", ":"+puertoRPC)
	if err != nil {
		log.Fatalf("Error al crear listener RPC en el puerto %s: %v", puertoRPC, err)
	}
	defer listener.Close()
	// Aceptamos conexiones concurrentemente
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error al aceptar conexión RPC: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

// TODO 17: Implementar bucleAntiEntropia.
// Cada 5 segundos, obtener un par con gossipNodo.AntiEntropia().
// Si hay par, conectarse via RPC, intercambiar miembros y fusionar.
func bucleAntiEntropia() {
		ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		// 1. Obtener un par aleatorio de la lista de miembros conocidos
		par := gossipNodo.AntiEntropia()
		if par == "" {
			continue
		}
		// 2. Conectarse vía RPC al par seleccionado
		c, err := rpc.Dial("tcp", par)
		if err != nil {
			log.Printf("[GOSSIP] Error al conectar con %s: %v", par, err)
			continue
		}
		// 3. Preparar los datos a enviar
		req := gossip.Intercambio{
			Remitente: miDireccion,
			Miembros:  gossipNodo.ObtenerMiembros(),
		}
		var resp gossip.Intercambio
		// 4. Realizar la llamada RPC
		err = c.Call("ServicioGossip.Intercambiar", req, &resp)
		c.Close()
		if err != nil {
			log.Printf("[GOSSIP] Error en intercambio RPC con %s: %v", par, err)
			continue
		}
		// 5. Fusionar los miembros devueltos en nuestro mapa local
		gossipNodo.FusionarMiembros(resp.Miembros)
		if resp.Remitente != "" {
			gossipNodo.Unirse(resp.Remitente)
		}
	}
}

// TODO 18: Implementar manejadorEstado.
// GET /estado devuelve JSON con node_id, miembros y finger_table.
func manejadorEstado(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var ft []string
	if chordNodo != nil {
		// Convertimos el array de la finger table a un slice
		ft = chordNodo.FingerTable[:]
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":      idNodo,
		"miembros":     gossipNodo.ObtenerMiembros(),
		"finger_table": ft,
	})
}
// TODO 19: Implementar manejadorAlmacenar.
func manejadorAlmacenar(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Clave int    `json:"clave"`
		Valor string `json:"valor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// 1. Si somos responsables de la clave, la guardamos localmente
	if chordNodo.EsResponsable(req.Clave) {
		chordNodo.Almacenar(req.Clave, req.Valor)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":           "success",
			"nodo_responsable": miDireccion,
			"nodo_id":          idNum,
		})
		return
	}
	// 2. Si no, reenviamos la solicitud al sucesor vía RPC
	c, err := rpc.Dial("tcp", chordNodo.Sucesor)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error conectando al sucesor: %v", err), http.StatusInternalServerError)
		return
	}
	defer c.Close()
	args := dht.ArgsStore{Clave: req.Clave, Valor: req.Valor}
	var resp dht.RespStore
	err = c.Call("ServicioChord.Almacenar", args, &resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error RPC al almacenar: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":           "success",
		"nodo_responsable": resp.NodoResponsable,
		"nodo_id":          resp.NodoID,
	})
}
// TODO 20: Implementar manejadorBuscar.
func manejadorBuscar(w http.ResponseWriter, r *http.Request) {
	claveStr := r.URL.Query().Get("clave")
	clave, err := strconv.Atoi(claveStr)
	if err != nil {
		http.Error(w, "Clave inválida", http.StatusBadRequest)
		return
	}
	// 1. Si somos responsables de la clave, la leemos localmente
	if chordNodo.EsResponsable(clave) {
		valor, existe := chordNodo.Obtener(clave)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"valor":            valor,
			"encontrado":       existe,
			"nodo_responsable": miDireccion,
			"nodo_id":          idNum,
		})
		return
	}
	// 2. Si no, reenviamos la consulta al sucesor vía RPC para que busque usando la Finger Table
	c, err := rpc.Dial("tcp", chordNodo.Sucesor)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error conectando al sucesor: %v", err), http.StatusInternalServerError)
		return
	}
	defer c.Close()
	args := dht.ArgsLookup{Clave: clave}
	var resp dht.RespLookup
	err = c.Call("ServicioChord.Obtener", args, &resp)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error RPC al buscar: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"valor":            resp.Valor,
		"encontrado":       resp.Encontrado,
		"nodo_responsable": resp.NodoResponsable,
		"nodo_id":          resp.NodoID,
	})
}
// bucleEstabilizacionChord recalcula el anillo Chord cada 10 segundos.
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