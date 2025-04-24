import os
from core.auth import autenticar
from core.importador_lote import importar_eventos_lote
from endpoints.drivers import importar_drivers
from endpoints.assets import importar_assets
from endpoints.trips import importar_trips
from endpoints.subtrips import importar_subtrips
from endpoints.tipos_eventos import importar_tipos_eventos


if __name__ == "__main__":
    print("🔐 Autenticando na API...")
    autenticar()
    
    print("📄 Importando tipos de eventos...")
    importar_tipos_eventos()
    
    print("\n🚚 Importando eventos TR...")
    importar_eventos_lote()

    print("\n👨‍✈️ Importando motoristas...")
    importar_drivers()

    print("\n🚗 Importando ativos (assets)...")
    importar_assets()

    print("\n🧭 Importando viagens (trips)...")
    importar_trips()

    print("\n✅ Importação completa.")
