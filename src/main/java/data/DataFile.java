package data;

public enum DataFile {
	WIND	("wind_generation"),
	SOLAR	("solar_generation"),

	// Development
	WIND_1000ROWS			("wind_generation_1000rows"),
	WIND_100ROWS			("wind_generation_100rows"),
	WIND_100ROWS_3COUNTRIES	("wind_generation_100rows_3countries"),
	WIND_10ROWS_2COUNTRIES	("wind_generation_10rows_2countries"),
	WIND_10ROWS_3COUNTRIES	("wind_generation_10rows_3countries"),
	WIND_10ROWS_4COUNTRIES	("wind_generation_10rows_4countries"),
	WIND_10ROWS_7COUNTRIES	("wind_generation_10rows_7countries");

	private final String fileName;

	DataFile(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}
}
