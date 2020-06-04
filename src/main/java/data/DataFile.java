package data;

public enum DataFile {
	// Main
	WIND	("wind"),
	SOLAR	("solar"),

	// Discretized
	WIND_DISCRETIZED			("wind_equal_frequency"),
	SOLAR_DISCRETIZED			("solar_equal_frequency"),

	// Reduced
	SOLAR_1YEAR  ("solar_continuous_13country_1years"),
	SOLAR_2YEAR("solar_continuous_13country_2years"),
	SOLAR_3YEAR  ("solar_continuous_13country_3years"),
	SOLAR_4YEAR("solar_continuous_13country_4years"),

	WIND_1000ROWS				("wind_1000rows"),
	WIND_100ROWS				("wind_100rows"),
	WIND_100ROWS_3COUNTRIES		("wind_100rows_3countries"),
	WIND_10ROWS_2COUNTRIES		("wind_10rows_2countries"),
	WIND_10ROWS_3COUNTRIES		("wind_10rows_3countries"),
	WIND_10ROWS_4COUNTRIES		("wind_10rows_4countries"),
	WIND_10ROWS_7COUNTRIES		("wind_10rows_7countries"),
	WIND_10ROWS_3COUNTRIES_DISC	("wind_10rows_3countries_disc"),
	WIND_10ROWS_6COUNTRIES_DISC	("wind_10rows_6countries_disc"),
	WIND_100ROWS_3COUNTRIES_DISC("wind_100rows_3countries_disc"),
	WIND_100ROWS_6COUNTRIES_DISC("wind_100rows_6countries_disc");

	private final String fileName;

	DataFile(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}
}
