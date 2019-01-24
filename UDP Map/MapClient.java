/*
 * Name: Chengyue Gong
 * Date created: September 1
 * Date last modified: September 2
 * Usage: MapClient hostname port method argument1 (argument2)
 *
 * Description: The client sends a request (a UDP packet) to the server
 * and prints the message (the payload of a UDP packet) returned by the server.
 * The server stores a set of (key, value) pairs.
 *
 * The types of request include get, put, and remove.
 * get:k	- returns the value of the key=k if key=k exists
 * put:k:v 	- adds the pair (k,v) (if key=k exists, replaces the value)
 * remove:k - deletes the pair (k,v) if key=k exists
 */

import java.io.*;
import java.net.*;

public class MapClient {
	public static void main(String args[]) throws Exception {
		// 1. Get server address and port number
		InetAddress serverAdr = InetAddress.getByName(args[0]);
		int port = Integer.parseInt(args[1]);

		// 2. Open UDP socket
		DatagramSocket sock = new DatagramSocket();

		// 3. Build packet addressed to server, encoded using US-ASCII Charset
		String operation = args[2]; // operation = get, put, or remove
		String key = args[3];
		String payload = operation + ":" + key;
		// value field required for put operation
		if (args.length > 4) payload = payload + ":" + args[4];
		byte[] outBuf = payload.getBytes("US-ASCII");
		DatagramPacket outPkt = new DatagramPacket(outBuf, outBuf.length, 
							   serverAdr, port);
		// Send packet to server
		sock.send(outPkt);

		// 4. Create buffer and packet for reply
		byte[] inBuf = new byte[1000];
		DatagramPacket inPkt = new DatagramPacket(inBuf, inBuf.length);
		// Wait for reply
		sock.receive(inPkt);

		// 5. Print the payload of the packet received from the server
		String reply = new String(inBuf, 0, inPkt.getLength(), "US-ASCII");
		System.out.println(reply);
		// Close socket
		sock.close();
	}
}