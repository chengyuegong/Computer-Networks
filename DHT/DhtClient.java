/* DhtClient.java
 * Author: Chengyue Gong
 * Date created: October 6
 * Date last modified: October 8
 * Usage: DhtClient myIp cfgFile <operation> key [value]
 *
 * myIp			is the IP address of the interface that 
 * the client should bind to its own datagram socket
 * cfgFile 		is configuration file containing 
 * the IP address and port number used by a DhtServer 
 * operation    is "put" or "get"
 * value  		is optional
 *
 * Description: The DhtClient sends a request (a UDP packet) to the DhtServer.
 * DhtServer stores (key,value) pairs.
 * The request is get or put.
 *
 * The types of request include get and put.
 * get k	- receive a "success" packet with the value if key=k exists
 * 			- receive a "no match" packet if key=k doesn't exist
 * put k v 	- adds the pair (k,v) to the DHT
 *			  (if key=k exists, replace the value)
 * 			  (if v is ommitted, remove the pair with key=k)
 */

import java.io.*;
import java.net.*;

public class DhtClient {
	public static void main(String args[]) throws Exception {
		final int sendTag = 12345; // tag for packets
		boolean debug = true; // enable debug messages
		// process command-line arguments
		if (args.length < 3) {
			System.err.println("usage: DhtClient myIp cfgFile " +
					   "<operation> key [value]");
			System.exit(1);
		}
		String cfgFile = args[1]; // configuration file (server IP + port)
		String op = args[2]; // "get" or "put"
		String key = null; // key
		if (args.length > 3)
			key = args[3];
		String value = null; // value
		if (args.length > 4)
			value = args[4];
		
		// open socket for sending and receiving packets
		// read server ip and port from configuration file
		InetAddress myIp = null; DatagramSocket sock = null;
		InetSocketAddress serverAdr = null;
		try {	
			myIp = InetAddress.getByName(args[0]); // my ip address
			sock = new DatagramSocket(0, myIp); // create a socket
			BufferedReader cfg = new BufferedReader(
						new InputStreamReader(
						new FileInputStream(cfgFile),
						"US-ASCII"));
			String s = cfg.readLine();
			String[] chunks = s.split(" ");
			serverAdr = new InetSocketAddress(
					chunks[0],Integer.parseInt(chunks[1])); // server address
		} catch(Exception e) {
			System.err.println("Error when creating socket" +
						"and reading sever address");
			System.exit(1);
		}

		// send packet
		Packet sendPkt = new Packet();
		sendPkt.type = op; // get or put
		sendPkt.tag = sendTag; // default value for tag
		if (key != null)
			sendPkt.key = key;
		if (value != null)
			sendPkt.val = value;
		sendPkt.send(sock, serverAdr, debug);

		// receive packet
		Packet replyPkt = new Packet();
		replyPkt.receive(sock, debug);
		
		// close socket
		sock.close();
	}
}