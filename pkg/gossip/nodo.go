package gossip

import (
	
	"sync"
	
)

// TODO 1: Definir el struct NodoGossip.
// Debe mantener una lista de miembros (map[string]bool) protegida por sync.RWMutex.
// Campos sugeridos:
//   - ID int
//   - Dirección string (ej. "localhost:5000")
//   - Miembros map[string]bool (todos los nodos conocidos, incluido si mismo)
//   - mu sync.RWMutex

type NodoGossip struct {
	ID        int
	Direccion string
	Miembros  map[string]bool
	mu        sync.RWMutex
}

// TODO 2: Implementar NuevoNodo.
// Crear un nodo con ID y Direccion dados. Inicializar Miembros con solo la Direccion propia.
func NuevoNodo(id int, direccion string) *NodoGossip {
	return &NodoGossip{
		ID:        id,
		Direccion: direccion,
		Miembros:  map[string]bool{direccion: true}, // El nodo arranca conociéndose solo a sí mismo
	}
 }

// TODO 3: Implementar Unirse.
// Agrega una Direccion a la lista de miembros (protegido con mutex).
func (n *NodoGossip) Unirse(direccion string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Miembros[direccion] = true
}

// TODO 4: Implementar AntiEntropia.
// Devuelve la Direccion de un par aleatorio de Miembros (distinto de si mismo).
// Si no hay otros miembros, retorna "".
// Esta función sera llamada periódicamente por el main para realizar el RPC.
// Pasos:
//   1. Adquirir RLock sobre n.mu.
//   2. Iterar n.Miembros buscando una Direccion distinta de n.Direccion.
//   3. Si se encuentra, retornarla. Si no, retornar "".
func (n *NodoGossip) AntiEntropia() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for dir := range n.Miembros {
		if dir != n.Direccion {
			return dir
		}
	}
	return ""
	
}

// TODO 5: Implementar ObtenerMiembros.
// Devuelve una copia de la lista de miembros (protegida con RLock).
func (n *NodoGossip) ObtenerMiembros() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	miembros := make([]string, 0, len(n.Miembros))
	for dir := range n.Miembros {
		miembros = append(miembros, dir)
	}
	return miembros
}

// TODO 6: Implementar FusionarMiembros.
// Recibe un slice de direcciones y las agrega a Miembros.
func (n *NodoGossip) FusionarMiembros(nuevos []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, dir := range nuevos {
		n.Miembros[dir] = true
	}
}

// Intercambio es la estructura usada en RPC para intercambiar miembros.
type Intercambio struct {
	Remitente string
	Miembros  []string
}

// ArgsVacio es un argumento RPC vacío (para llamadas sin parámetros).
type ArgsVacio struct{}

// RespIdentificacion devuelve la Direccion e ID lógico del nodo consultado.
// Se usa en dos lugares:
//   1) Al unirse via SEED: normaliza la Direccion del seed para evitar
//      duplicados en la membresía (e.g. "localhost:5000" vs "host:5000").
//   2) En la estabilización periódica del anillo Chord: permite a cada nodo
//      conocer el ID lógico Chord de sus vecinos descubiertos por Gossip,
//      para poder recolocarlos en el anillo (los IDs no se derivan del
//      puerto porque varios nodos pueden compartir el puerto 5000 en Docker).
type RespIdentificacion struct {
	Direccion string
	ID        int
}

// ServicioGossip es el servicio RPC para Gossip.
type ServicioGossip struct {
	Nodo *NodoGossip
}

// Identificarse devuelve la Direccion con la que este nodo se identifica a si
// mismo y su ID lógico Chord. Permite que otros nodos normalicen referencias
// (evitando duplicados como "localhost:5000" vs "hostname:5000") y que el
// bucle de estabilización de Chord descubra el ID lógico de cada vecino.
// RESUELTO: lo usa bucleEstabilizacionChord del main.
func (s *ServicioGossip) Identificarse(_ ArgsVacio, resp *RespIdentificacion) error {
	resp.Direccion = s.Nodo.Direccion
	resp.ID = s.Nodo.ID
	return nil
}

// Intercambiar recibe los miembros de otro nodo y devuelve los propios.
// TODO 7: implementar anti-entropía (push-pull).
func (s *ServicioGossip) Intercambiar(req Intercambio, resp *Intercambio) error {
	 // Agregamos al remitente y todos sus miembros a nuestra lista
    s.Nodo.Unirse(req.Remitente)
    s.Nodo.FusionarMiembros(req.Miembros)
    // Respondemos con nuestros propios miembros y dirección
    resp.Remitente = s.Nodo.Direccion
    resp.Miembros = s.Nodo.ObtenerMiembros()
    return nil
}
