# src/exceptions.py

class PredictionException(Exception):
    """Excepción personalizada para errores de predicción en BasketModel."""
    pass

class UserNotFoundException(Exception):
    """Excepción personalizada para usuario no encontrado en FeatureStore."""
    pass
