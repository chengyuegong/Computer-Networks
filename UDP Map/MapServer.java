/*
 * Name: Chengyue Gong
 * Date created: September 1
 * Date last modified: September 2
 * Usage: MapServer (port)
 *
 * Description: The server stores a set of (key, value) pair.
 * Keys and values are strings, and no same key.
 * The server receives the request (a UDP packet) from the client,
 * and replies with the message (a UDP packet) indicating 
 * whether the request has been successfully completed or not.
 *
 * The types of request include get, put, and remove.
 * get:k	- returns the value of the key=k if key=k exists
 * put:k:v 	- adds the pair (k,v) (if key=k exists, replaces the value)
 * remove:k - deletes the pair (k,v) if key=k exists
 */

import java.io.*;
import java.net.*;
import java.util.HashMap;

public class MapServer {
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

 	// put function (put:k:v)
 	// Found k 		- updated:key
 	// Not Found k	- Ok
	private static String put(String key, String value) {
 		if (hmap.containsKey(key)) {
 			hmap.replace(key, value);
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

	public static void main(String args[]) throws Exception {

		// 1. Create a DatagramSocket and bind it to the specified port
		int port = 30123; // the default port number
		if (args.length > 0) port = Integer.parseInt(args[0]);

		// 2. Open UDP socket
		DatagramSocket sock = new DatagramSocket(port);

		// 3. Create a Datagrampacket for receiving packets
		byte[] buf = new byte[1000];
		DatagramPacket pkt = new DatagramPacket(buf, buf.length);

		// 4. Response the packet from the client
		while (true) {
			// Recover the length of the packet
			pkt.setData(buf);
			// Wait for incoming packet
			sock.receive(pkt);
			// Get the request from the client (the payload of the packet)
			String payload = new String(buf, 0, pkt.getLength(), "US-ASCII");

			// PROCESS the request
			String response = new String();
			String[] request = payload.split(":");
			// Processing "get" request (get:k)
			if (request[0].equals("get") && request.length == 2) { 
				String key = request[1];
				response = get(key);
			} 
			// Processing "put" request (put:k:v)
			else if (request[0].equals("put") && request.length == 3) { 
				String key = request[1];
				String value = request[2];
				response = put(key, value);
			} 
			// Processing "remove" request (remove:k)
			else if (request[0].equals("remove") && request.length == 2) { 
				String key = request[1];
				response = remove(key);
			} 
			// Improperly formatted commands
			// Error:unrecognizable input:the input’s packet payload
			else {
				response = "Error:unrecognizable input:" + payload;
			}

			// Prepare the packet for response
			pkt.setData(response.getBytes("US-ASCII"));
			// Reply
			sock.send(pkt);
		}
	}
}