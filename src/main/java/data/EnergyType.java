package data;

public enum EnergyType {
	WIND	("wind_generation"),
	SOLAR	("solar_generation");

	private final String fileName;

	EnergyType(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}
}
