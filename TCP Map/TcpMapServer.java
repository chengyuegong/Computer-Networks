/* TcpMapServer.java
 * Author: Chengyue Gong
 * Date created: September 10
 * Date last modified: September 15
 * Usage: TcpMapServer [ IP [port] ]
 * If IP address is omitted, the wildcard address is used.
 * If port number is omitted, it will be set to 30123.
 *
 * Description: The server stores a set of (key, value) pairs.
 * Keys and values are strings, and no same key.
 * The server creates a TCP server socket listenning to the client, and
 * creates a dedicated socket for the client when receiving a connection request.
 * Then it processes multiple operations from the client,
 * and replies with the message indicating whether the operations
 * have been successfully completed or not.
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
import java.util.HashMap;

public class TcpMapServer {
	// Store a set of (key, value) pairs
	private static HashMap<String, String> hmap = new HashMap<>();

 	// get function (get:k)
 	// Found k 		- ok:value
 	// Not Found k	- no match
 	private static String get(String key) {
 		if (hmap.containsKey(key)) {
 			return "ok:" + hmap.get(key);
 		} else {
 			return "no match";
 		}
 	}

 	// get all function (get all)
 	// Return all key-value pairs
 	private static String getAll() {
 		StringBuilder all = new StringBuilder();
 		int size = hmap.size();
 		int count = 0;
 		for (String key: hmap.keySet()) {
 			all.append(key+":"+hmap.get(key));
 			if (count < size - 1)
 				all.append("::");
 			count++;
 		}
 		return all.toString();
 	}

 	// put function (put:k:v)
 	// Found k 		- updated:key
 	// Not Found k	- Ok
	private static String put(String key, String value) {
 		if (hmap.containsKey(key)) {
 			hmap.put(key, value);
 			return "updated:" + key;
 		} else {	
 			hmap.put(key, value);
 			return "Ok";
 		}
 	}

 	// remove function (remove:k)
 	// Found k 		- Ok
 	// Not Found k	- no match
 	private static String remove(String key) {
 		if (hmap.containsKey(key)) {
 			hmap.remove(key);
 			return "Ok";
 		} else {
 			return "no match";
 		}
 	}

 	// Process the operation
 	private static String processOperation(String command) {
 		String response;
		String[] request = command.split(":");
		int numberOfArgument = request.length;
		int numberOfColon = 0;
		for (int i = 0; i < command.length(); i++) {
			if (command.charAt(i) == ':')
				numberOfColon++;
		}
		// Processing "get" request (get:k)
		if (request[0].equals("get") 
			&& numberOfArgument == 2 
			&& numberOfColon == 1) { // prevent redundant colon(:)
			String key = request[1];
			response = get(key);
		} 
		// Processing "get all" request (get all)
		else if (request[0].equals("get all") 
			&& numberOfArgument == 1
			&& numberOfColon == 0) { // prevent redundant colon(:)
			response = getAll();
		}
		// Processing "put" request (put:k:v)
		else if (request[0].equals("put") 
			&& numberOfArgument == 3
			&& numberOfColon == 2) { // prevent redundant colon(:)
			String key = request[1];
			String value = request[2];
			response = put(key, value);
		} 
		// Processing "remove" request (remove:k)
		else if (request[0].equals("remove") 
			&& numberOfArgument == 2
			&& numberOfColon == 1) { // prevent redundant colon(:)
			String key = request[1];
			response = remove(key);
		} 
		// Improperly formatted commands
		// Error:unrecognizable input:the input’s packet payload
		else {
			response = "Error:unrecognizable input:" + command;
		}
		return response;
 	}

	public static void main(String args[]) throws Exception {
		// 1. Oepn listening socket
		int port = 30123; // default port number
		InetAddress serverAdr = null; // wildcard address
		if (args.length > 1)
			port = Integer.parseInt(args[1]);
		if (args.length > 0) 
			serverAdr = InetAddress.getByName(args[0]);
		ServerSocket listenSock = new ServerSocket(port, 0, serverAdr);

		// 2. Communication begins
		while (true) {
			// 2.1 Create a new socket for incoming connection request
			Socket sock = listenSock.accept();
			// 2.2 Create buffered reader & writer for socket's io streams
			BufferedReader in = new BufferedReader(new InputStreamReader(
				sock.getInputStream(),"US-ASCII"));
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
				sock.getOutputStream(),"US-ASCII"));
			// 2.3 Process operations
			while (true) {
				// Get the command
				String command = in.readLine();
				// Check if it is a blank line
				if (command == null || command.length() == 0) 
					break;
				// PROCESS
				String response = processOperation(command);
				// Reply
				out.write(response);
				out.newLine();
				out.flush();
			}
			// 2.4 Close the socket
			sock.close();
		}
	}
}