package dht

import "testing"

func TestEsResponsableYMejorSalto(t *testing.T) {
	n := NuevoNodo(50, "localhost:5000", "localhost:5001", 100, "localhost:4999", 1)
	if n == nil {
		t.Fatal("NuevoNodo devolvió nil")
	}
	if !n.EsResponsable(2) {
		t.Fatal("esperaba que el nodo 50 fuera responsable de la clave 2")
	}
	if n.EsResponsable(51) {
		t.Fatal("no debería ser responsable de la clave 51")
	}

	if got := n.MejorSalto(75); got != "localhost:5001" {
		t.Fatalf("MejorSalto = %q, want %q", got, "localhost:5001")
	}
}

func TestAlmacenarYObtener(t *testing.T) {
	n := NuevoNodo(50, "localhost:5000", "localhost:5001", 100, "localhost:4999", 1)
	n.Almacenar(10, "hola")
	if got, ok := n.Obtener(10); !ok || got != "hola" {
		t.Fatalf("Obtener(10) = %q, %v, want %q, true", got, ok, "hola")
	}
	if _, ok := n.Obtener(11); ok {
		t.Fatal("esperaba ausencia de clave no almacenada")
	}
}
