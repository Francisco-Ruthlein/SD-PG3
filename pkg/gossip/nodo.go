package gossip

import (
	"math/rand"
	"sort"
	"sync"
)

// NodoGossip mantiene la membresía descubierta por Gossip.
type NodoGossip struct {
	ID        int
	Direccion string
	Miembros  map[string]bool
	mu        sync.RWMutex
}

// NuevoNodo crea un nodo con solo su propia dirección en la membresía.
func NuevoNodo(id int, direccion string) *NodoGossip {
	if direccion == "" {
		return nil
	}
	return &NodoGossip{
		ID:        id,
		Direccion: direccion,
		Miembros:  map[string]bool{direccion: true},
	}
}

// Unirse agrega una dirección a la membresía si no estaba presente.
func (n *NodoGossip) Unirse(direccion string) {
	if n == nil || direccion == "" {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Miembros[direccion] = true
}

// AntiEntropia devuelve una dirección aleatoria de otro nodo conocido.
func (n *NodoGossip) AntiEntropia() string {
	if n == nil {
		return ""
	}
	n.mu.RLock()
	defer n.mu.RUnlock()

	candidatos := make([]string, 0, len(n.Miembros))
	for miembro := range n.Miembros {
		if miembro != n.Direccion {
			candidatos = append(candidatos, miembro)
		}
	}
	if len(candidatos) == 0 {
		return ""
	}
	return candidatos[rand.Intn(len(candidatos))]
}

// ObtenerMiembros devuelve una copia ordenada de la membresía actual.
func (n *NodoGossip) ObtenerMiembros() []string {
	if n == nil {
		return []string{}
	}
	n.mu.RLock()
	defer n.mu.RUnlock()

	miembros := make([]string, 0, len(n.Miembros))
	for miembro := range n.Miembros {
		miembros = append(miembros, miembro)
	}
	sort.Strings(miembros)
	return miembros
}

// FusionarMiembros incorpora nuevas direcciones a la membresía.
func (n *NodoGossip) FusionarMiembros(nuevos []string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, direccion := range nuevos {
		if direccion != "" {
			n.Miembros[direccion] = true
		}
	}
}

// Intercambio se usa en RPC para intercambiar miembros entre nodos.
type Intercambio struct {
	Remitente string
	Miembros  []string
}

// ArgsVacio es un argumento RPC vacío.
type ArgsVacio struct{}

// RespIdentificacion devuelve la dirección e ID lógico del nodo consultado.
type RespIdentificacion struct {
	Direccion string
	ID        int
}

// ServicioGossip expone los métodos RPC para gossip.
type ServicioGossip struct {
	Nodo *NodoGossip
}

// Identificarse devuelve la dirección e ID lógico del nodo consultado.
func (s *ServicioGossip) Identificarse(_ ArgsVacio, resp *RespIdentificacion) error {
	if s == nil || s.Nodo == nil {
		return nil
	}
	resp.Direccion = s.Nodo.Direccion
	resp.ID = s.Nodo.ID
	return nil
}

// Intercambiar realiza anti-entropía push-pull entre dos nodos.
func (s *ServicioGossip) Intercambiar(req Intercambio, resp *Intercambio) error {
	if s == nil || s.Nodo == nil {
		return nil
	}
	s.Nodo.Unirse(req.Remitente)
	s.Nodo.FusionarMiembros(req.Miembros)
	resp.Remitente = s.Nodo.Direccion
	resp.Miembros = s.Nodo.ObtenerMiembros()
	return nil
}
