package es.upv.pros.mlgenerator;

public enum ModelType {
  Classification,
  Regression,
  Clustering;

  public static ModelType from(String raw) {
    if (raw == null) {
      throw new IllegalArgumentException("Falta el atributo obligatorio 'type' en el modelo.");
    }
    try {
      return ModelType.valueOf(raw);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "type inválido: '" + raw + "'. Valores permitidos: Classification, Regression, Clustering");
    }
  }
}
