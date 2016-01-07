package org.apache.nutch.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * The IPLocator is a java wrapper for the hostip.info ip locator web service.
 *
 * @author <a href="mailto:pillvin@iit.edu">Vinod Pillai</a>
 * @version $Revision: 1.0 $
 */
public class IPLocator {

  private static final String HOSTIP_LOOKUP_URL = "http://api.hostip.info/get_html.php?position=true&ip=";
  private static final String CRLF = "\r\n";
  private static final String KEY_COUNTRY = "Country";
  private static final String KEY_CITY = "City";
  private static final String KEY_LATITUDE = "Latitude";
  private static final String KEY_LONGITUDE = "Longitude";

  private String city;
  private String country;
  private float longitude;
  private float latitude;

  /**
   * This is a singleton class therefore make the constructor private
   */
  private IPLocator() {}

  /**
   * Other than the getters & setters, this is the only method visible to the outside world
   *
   * @param ip The ip address to be located
   * @return IPLocator instance
   * @throws Exception in case of any error/exception
   */
  public static IPLocator locate(String ip) throws Exception {
    String url = HOSTIP_LOOKUP_URL + ip;
    URL u = new URL(url);
    IPLocator ipl = new IPLocator();

    String response = ipl.getContent(u);

    String[] tokens = response.split(CRLF);

    String TOKEN_DELIMITER=":";
    for(int i = 0; i < tokens.length; i++) {
      String token = tokens[i].trim();
      String[] keyValue = token.split(TOKEN_DELIMITER);
      if (keyValue.length!=2) continue;
      String key = keyValue[0];
      String value = keyValue[1];
      if(key.equalsIgnoreCase(KEY_COUNTRY)) {
        ipl.setCountry(value);
      }
      else if(key.equalsIgnoreCase(KEY_CITY)) {
        ipl.setCity(value);
      }
      else if(key.equalsIgnoreCase(KEY_LATITUDE)) {
        try {
          ipl.setLatitude(Float.parseFloat(value));
        }
        catch(Exception e) {
          ipl.setLatitude(-1);
        }
      }
      else if(key.equalsIgnoreCase(KEY_LONGITUDE)) {
        try {
          ipl.setLongitude(Float.parseFloat(value));
        }
        catch(Exception e) {
          ipl.setLongitude(-1);
        }
      }
    }
    return ipl;
  }

  /**
   * Gets the content for a given url. This method makes a connection, gets the response from the url.
   *
   * A RuntimeException is throws is the status code of the response is not 200.
   *
   * @param url The url to open.
   * @return HTML response
   */
  private String getContent(URL url) throws  Exception {

    HttpURLConnection http = (HttpURLConnection) url.openConnection ();
    http.connect ();

    int code = http.getResponseCode();
    if(code != 200) throw new RuntimeException("IP Locator failed to get the location. Http Status code : " + code);
    return this.getContent(http);
  }

  /**
   * Gets the content for a given HttpURLConnection.
   *
   * @param connection Http URL Connection.
   * @return HTML response
   */

  private String getContent(HttpURLConnection connection) throws IOException {
    InputStream in = connection.getInputStream ();
    StringBuffer sbuf = new StringBuffer();
    InputStreamReader isr = new InputStreamReader ( in );
    BufferedReader bufRead = new BufferedReader ( isr );

    String aLine = null;
    do {
      aLine = bufRead.readLine();
      if ( aLine != null ) {

        sbuf.append(aLine).append(CRLF);
      }
    }
    while ( aLine != null );

    return sbuf.toString();
  }

  /**
   * @return the city
   */
  public String getCity() {
    return city;
  }

  /**
   * @param city the city to set
   */
  public void setCity(String city) {
    this.city = city;
  }

  /**
   * @return the country
   */
  public String getCountry() {
    return country;
  }

  /**
   * @param country the country to set
   */
  public void setCountry(String country) {
    this.country = country;
  }

  /**
   * @return the longitude
   */
  public float getLongitude() {
    return longitude;
  }

  /**
   * @param longitude the longitude to set
   */
  public void setLongitude(float longitude) {
    this.longitude = longitude;
  }

  /**
   * @return the latitude
   */
  public float getLatitude() {
    return latitude;
  }

  /**
   * @param latitude the latitude to set
   */
  public void setLatitude(float latitude) {
    this.latitude = latitude;
  }


  /**
   * For unit testing purposes only
   *
   * @param args
   */
  public static void main(String args[]) {

    try {
      IPLocator ipl = IPLocator.locate("12.215.42.19");
      System.out.println("City="+ipl.getCity());
      System.out.println("Country="+ipl.getCountry());
      System.out.println("Latitude="+ipl.getLatitude());
      System.out.println("Longitude="+ipl.getLongitude());
    }
    catch(Exception e) {
      e.printStackTrace();
    }
  }
}
