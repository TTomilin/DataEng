package data;

public enum DataFile {
	WIND	("wind"),
	SOLAR	("solar"),

	// Development
	WIND_1000ROWS				("wind_1000rows"),
	WIND_100ROWS				("wind_100rows"),
	WIND_100ROWS_3COUNTRIES		("wind_100rows_3countries"),
	WIND_10ROWS_2COUNTRIES		("wind_10rows_2countries"),
	WIND_10ROWS_3COUNTRIES		("wind_10rows_3countries"),
	WIND_10ROWS_4COUNTRIES		("wind_10rows_4countries"),
	WIND_10ROWS_7COUNTRIES		("wind_10rows_7countries"),
	WIND_10ROWS_3COUNTRIES_DISC	("wind_10rows_3countries_disc"),
	WIND_10ROWS_6COUNTRIES_DISC	("wind_10rows_6countries_disc");

	private final String fileName;

	DataFile(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return fileName;
	}
}
