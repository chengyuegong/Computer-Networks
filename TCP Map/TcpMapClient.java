/* TcpMapClient.java
 * Author: Chengyue Gong
 * Date created: September 10
 * Date last modified: September 10
 * Usage: TcpMapClient IP/hostname [port]
 * The port number defaults to 30123.
 *
 * Description: The client first sends a TCP connection request to the server.
 * After connection establishes, the client sends multiple requests to the server
 * and prints the message returned by the server.
 * The server stores a set of (key, value) pairs.
 * The server and the client communicate via socket inputstream and outputstream.
 *
 * The types of request include get, get all, put, and remove.
 * get:k	- returns the value of the key=k if key=k exists
 * get all  - returns all of the key-value pairs
 * put:k:v 	- adds the pair (k,v) (if key=k exists, replaces the value)
 * remove:k - deletes the pair (k,v) if key=k exists
 */

import java.io.*;
import java.net.*;

public class TcpMapClient {
	public static void main(String args[]) throws Exception {
		// 1. Create a socket for TCP connection to server
		int port = 30123; // default port number
		if (args.length > 1) 
			port = Integer.parseInt(args[1]);
		Socket sock = new Socket(args[0], port);
		
		// 2. Create buffered reader & writer for socket's io streams
		BufferedReader in = new BufferedReader(new InputStreamReader(
			sock.getInputStream(),"US-ASCII"));
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
			sock.getOutputStream(),"US-ASCII"));

		// 3. Create buffered reader for System.in
		BufferedReader sin = new BufferedReader(new InputStreamReader(
			System.in));

		// 4. Communication begins
		String line;
		while (true) {
			// 4.1 Enter a command
			line = sin.readLine();
			// Check if it is a blank line
			if (line == null || line.length() == 0) 
				break;
			// 4.2 Send the command to the server
			out.write(line); 
			out.newLine(); 
			out.flush();
			// 4.3 Print the reply from the server
			System.out.println(in.readLine());
		}
		// 5. Close the socket
		sock.close();
	}
}
