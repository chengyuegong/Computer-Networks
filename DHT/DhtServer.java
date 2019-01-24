/** Server for simple distributed hash table that stores (key,value) strings.
 *  Modified by: Wentao Wang & Chengyue Gong
 *  Date last modified: October 8
 *
 *  usage: DhtServer myIp numRoutes cfgFile [ cache ] [ debug ] [ predFile ]
 *  
 *  myIp	is the IP address to use for this server's socket
 *  numRoutes	is the max number of nodes allowed in the DHT's routing table;
 *  		typically lg(numNodes)
 *  cfgFile	is the name of a file in which the server writes the IP
 *		address and port number of its socket
 *  cache	is an optional argument; if present it is the literal string
 *		"cache"; when cache is present, the caching feature of the
 *		server is enabled; otherwise it is not
 *  debug	is an optional argument; if present it is the literal string
 *		"debug"; when debug is present, a copy of every packet received
 *		and sent is printed on stdout
 *  predFile	is an optional argument specifying the configuration file of
 *		this node's predecessor in the DHT; this file is used to obtain
 *		the IP address and port number of the predecessor's socket,
 *		allowing this node to join the DHT by contacting predecessor
 *  
 *  The DHT uses UDP packets containing ASCII text. Here's an example of the
 *  UDP payload for a get request from a client.
 *  
 *  CSE473 DHTPv0.1
 *  type:get
 *  key:dungeons
 *  tag:12345
 *  ttl:100
 *  
 *  The first line is just an identifying string that is required in every
 *  DHT packet. The remaining lines all start with a keyword and :, usually
 *  followed by some additional text. Here, the type field specifies that
 *  this is a get request; the key field specifies the key to be looked up;
 *  the tag is a client-specified tag that is returned in the response; and
 *  can be used by the client to match responses with requests; the ttl is
 *  decremented by every DhtServer and if < 0, causes the packet to be discarded.
 *  
 *  Possible responses to the above request include:
 *  
 *  CSE473 DHTPv0.1
 *  type:success
 *  key:dungeons
 *  value:dragons
 *  tag:12345
 *  ttl:95
 *  
 *  or
 *  
 *  CSE473 DHTPv0.1
 *  type:no match
 *  key:dungeons
 *  tag:12345
 *  ttl:95
 *  
 *  Put requests are formatted similarly, but in this case the client typically
 *  specifies a value field (omitting the value field causes the pair with the
 *  specified key to be removed).
 *  
 *  The packet type "failure" is used to indicate an error of some sort; in 
 *  this case, the "reason" field provides an explanation of the failure. 
 *  The "join" type is used by a server to join an existing DHT. In the same
 *  way, the "leave" type is used by the leaving server to circle around the 
 *  DHT asking other servers to delete it from their routing tables.  The 
 *  "transfer" type is used to transfer (key,value) pairs to a newly added 
 *  server. The "update" type is used to update the predecessor, successor, 
 *  or hash range of another DHT server, usually when a join or leave even 
 *  happens. 
 *
 *  Other fields and their use are described briefly below
 *  clientAdr 	is used to specify the IP address and port number of the 
 *              client that sent a particular request; it is added to a request
 *              packet by the first server to receive the request, before 
 *              forwarding the packet to another node in the DHT; an example of
 *              the format is clientAdr:123.45.67.89:51349.
 *  relayAdr  	is used to specify the IP address and port number of the first
 *              server to receive a request packet from the client; it is added
 *              to the packet by the first server before forwarding the packet.
 *  hashRange 	is a pair of integers separated by a colon, specifying a range
 *              of hash indices; it is included in the response to a "join" 
 *              packet, to inform the new DHT server of the set of hash values
 *              it is responsible for; it is also included in the update packet
 *              to update the hash range a server is responsible for.
 *  succInfo  	is the IP address and port number of a server, followed by its
 *              first hash index; this information is included in the response
 *              to a join packet to inform the new DHT server about its 
 *              immediate successor; it’s also included in the update packet 
 *              to change the immediate successor of a DHT server; an example 
 *              of the format is succInfo:123.45.6.7:5678:987654321.
 *  predInfo	is also the IP address and port number of a server, followed
 *              by its first hash index; this information is included in a join
 *              packet to inform the successor DHT server of its new 
 *              predecessor; it is also included in update packets to update 
 *              the new predecessor of a server.
 *  senderInfo	is the IP address and port number of a DHT server, followed by
 *              its first hash index; this information is sent by a DHT to 
 *              provide routing information that can be used by other servers.
 *              It also used in leave packet to let other servers know the IP
 *              address and port number information of the leaving server.
 */

import java.io.*;
import java.net.*;
import java.util.*;
import java.lang.*;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class DhtServer {
	private static int numRoutes;	// number of routes in routing table
	private static boolean cacheOn;	// enables caching when true
	private static boolean debug;	// enables debug messages when true

	private static HashMap<String,String> map;	// key/value pairs
	private static HashMap<String,String> cache;	// cached pairs
	private static List<Pair<InetSocketAddress,Integer>> rteTbl;

	private static DatagramSocket sock;
	private static InetSocketAddress myAdr;
	private static InetSocketAddress predecessor; // DHT predecessor
	private static Pair<InetSocketAddress,Integer> myInfo; 
	private static Pair<InetSocketAddress,Integer> predInfo; 
	private static Pair<InetSocketAddress,Integer> succInfo; // successor
	private static Pair<Integer,Integer> hashRange; // my DHT hash range
	private static int sendTag;		// tag for new outgoing packets
	// flag for waiting leave message circle back
	private static boolean stopFlag;
	 
	/** Main method for DHT server.
	 *  Processes command line arguments, initializes data, joins DHT,
	 *  then starts processing requests from clients.
	 */
	public static void main(String[] args) {
		// process command-line arguments
		if (args.length < 3) {
			System.err.println("usage: DhtServer myIp numRoutes " +
					   "cfgFile [debug] [ predFile ] ");
			System.exit(1);
		}
		numRoutes = Integer.parseInt(args[1]);
		String cfgFile = args[2];
		cacheOn = debug = false;
		stopFlag = false;
		String predFile = null;
		for (int i = 3; i < args.length; i++) {
			if (args[i].equals("cache")) cacheOn = true;
			else if (args[i].equals("debug")) debug = true;
			else predFile = args[i];
		}
		
		// open socket for receiving packets
		// write ip and port to config file
		// read predecessor's ip/port from predFile (if there is one)
		InetAddress myIp = null; sock = null; predecessor = null;
		try {	
			myIp = InetAddress.getByName(args[0]);
			sock = new DatagramSocket(0,myIp);
			BufferedWriter cfg =
				new BufferedWriter(
				    new OutputStreamWriter(
					new FileOutputStream(cfgFile),
					"US-ASCII"));
			cfg.write("" +	myIp.getHostAddress() + " " +
					sock.getLocalPort());
			cfg.newLine();
			cfg.close();
			if (predFile != null) {
				BufferedReader pred =
					new BufferedReader(
					    new InputStreamReader(
						new FileInputStream(predFile),
						"US-ASCII"));
				String s = pred.readLine();
				String[] chunks = s.split(" ");
				predecessor = new InetSocketAddress(
					chunks[0],Integer.parseInt(chunks[1]));
			}
		} catch(Exception e) {
			System.err.println("usage: DhtServer myIp numRoutes " +
					   "cfgFile [ cache ] [ debug ] " +
					   "[ predFile ] ");
			System.exit(1);
		}
		myAdr = new InetSocketAddress(myIp,sock.getLocalPort());
		
		// initialize data structures	
		map = new HashMap<String,String>();
		cache = new HashMap<String,String>();
		rteTbl = new LinkedList<Pair<InetSocketAddress,Integer>>();

		// join the DHT (if not the first node)
		hashRange = new Pair<Integer,Integer>(0,Integer.MAX_VALUE);
		myInfo = null;
		succInfo = null;
		predInfo = null;
		sendTag = 12345; // +tag for packets
		if (predecessor != null) {
			join(predecessor);
		} else {
			myInfo = new Pair<InetSocketAddress,Integer>(myAdr,0);
			succInfo = new Pair<InetSocketAddress,Integer>(myAdr,0);
			predInfo = new Pair<InetSocketAddress,Integer>(myAdr,0);
		}


		// start processing requests from clients
		Packet p = new Packet();
		Packet reply = new Packet();
		InetSocketAddress sender = null;

		/* this function will be called if there's a "TERM" or "INT"
		 * captured by the signal handler. It simply execute the leave
		 * function and leave the program.
		 */ 
		SignalHandler handler = new SignalHandler() {  
		    public void handle(Signal signal) {  
		        leave();
		        System.exit(0);
		    }  
		};
		//Signal.handle(new Signal("KILL"), handler); // capture kill -9 signal
		Signal.handle(new Signal("TERM"), handler); // capture kill -15 signal
		Signal.handle(new Signal("INT"), handler); // capture ctrl+c
		
		while (true) {
			try { sender = p.receive(sock,debug);
			} catch(Exception e) {
				System.err.println("received packet failure");
				continue;
			}
			if (sender == null) {
				System.err.println("received packet failure");
				continue;
			}
			if (!p.check()) {
				reply.clear();
				reply.type = "failure";
				reply.reason = p.reason;
				reply.tag = p.tag;
				reply.ttl = p.ttl;
				reply.send(sock,sender,debug);
				continue;
			}
			handlePacket(p,sender);
		}
	}

	/** Hash a string, returning a 32 bit integer.
	 *  @param s is a string, typically the key from some get/put operation.
	 *  @return and integer hash value in the interval [0,2^31).
	 */
	public static int hashit(String s) {
		while (s.length() < 16) s += s;
		byte[] sbytes = null;
		try { sbytes = s.getBytes("US-ASCII"); 
		} catch(Exception e) {
			System.out.println("illegal key string");
			System.exit(1);
		}
		int i = 0;
		int h = 0x37ace45d;
		while (i+1 < sbytes.length) {
			int x = (sbytes[i] << 8) | sbytes[i+1];
			h *= x;
			int top = h & 0xffff0000;
			int bot = h & 0xffff;
			h = top | (bot ^ ((top >> 16)&0xffff));
			i += 2;
		}
		if (h < 0) h = -(h+1);
		return h;
	}

	/** Leave an existing DHT.
	 *  
	 *	Send a leave packet to it's successor and wait until stopFlag is 
	 * 	set to "true", which means leave packet is circle back.
	 *
	 *	Send an update packet with the new hashRange and succInfo fields to 
	 *  its predecessor, and sends an update packet with the predInfo 
	 *  field to its successor. 
	 *	
	 *	Transfers all keys and values to predecessor.  
	 *	Clear all the existing cache, map and rteTbl information
	 */
	public static void leave() {
		// ++code
		// send a "leave" packet to its successor
		// (fields: type, tag, senderInfo)
		Packet leavePkt = new Packet();
		leavePkt.type = "leave";
		leavePkt.tag = sendTag;
		leavePkt.senderInfo = myInfo;
		leavePkt.send(sock, succInfo.left, debug);
		// wait until stopFlag is set to "true"
		while (!stopFlag) {}

		// send an "update" packet to its predeccsor 
		// (fields: type, tag, succInfo, hashRange)
		Packet updatePredPkt = new Packet();
		updatePredPkt.type = "update";
		updatePredPkt.succInfo = succInfo;
		updatePredPkt.hashRange = 
			new Pair(predInfo.right, hashRange.right);
		updatePredPkt.send(sock, predInfo.left, debug);

		// send an "update" packet to its successor 
		// (fields: type, tag, predInfo)
		Packet updateSuccPkt = new Packet();
		updateSuccPkt.type = "update";
		updateSuccPkt.predInfo = predInfo;
		updateSuccPkt.send(sock, succInfo.left, debug);

		// transfer all keys and values to its predecessor
		for (String key : map.keySet()) {	
			// send a "transfer" packet to its predecessor
			// (fields: type, tag, key, val)
			Packet xferPkt = new Packet();
			xferPkt.type = "transfer";
			xferPkt.tag = sendTag;
			xferPkt.key = key;
			xferPkt.val = map.get(key);
			xferPkt.send(sock, predInfo.left, debug);
		}
		// claer map, cache and routing table
		map.clear(); cache.clear(); rteTbl.clear();
	}
	
	/** Handle a update packet from a prospective DHT node.
	 *  @param p is the received join packet
	 *  @param adr is the socket address of the host that
	 *  
	 *	The update message might contain infomation need update,
	 *	including predInfo, succInfo, and hashRange. 
	 *  And add the new Predecessor/Successor into the routing table.
	 *	If succInfo is updated, succInfo should be removed from 
	 *	the routing table and the new succInfo should be added
	 *	into the new routing table.
	 */
	public static void handleUpdate(Packet p, InetSocketAddress adr) {
		if (p.predInfo != null){
			predInfo = p.predInfo;
		}
		if (p.succInfo != null){
			succInfo = p.succInfo;
			addRoute(succInfo);
		}
		if (p.hashRange != null){
			hashRange = p.hashRange;
		}
	}

	/** Handle a leave packet from a leaving DHT node.
	*  @param p is the received join packet
	*  @param adr is the socket address of the host that sent the leave packet
	*
	*  If the leave packet is sent by this server, set the stopFlag.
	*  Otherwise firstly send the received leave packet to its successor,
	*  and then remove the routing entry with the senderInfo of the packet.
	*/
	public static void handleLeave(Packet p, InetSocketAddress adr) {
		if (p.senderInfo.equals(myInfo)){
			stopFlag = true;
			return;
		}
		// send the leave message to successor 
		p.send(sock, succInfo.left, debug);

		// remove the senderInfo from route table
		removeRoute(p.senderInfo);
	}
	
	/** Join an existing DHT.
	 *  @param predAdr is the socket address of a server in the DHT,
	 *  
	 *  ++documentation
	 *	Send a join packet to its predecessor.
	 *  Wait for a "success" packet
	 */
	public static void join(InetSocketAddress predAdr) {
		// ++code
		// send a "join" packet to its predecessor
		// (fields: type, tag)
		Packet joinPkt = new Packet();
		joinPkt.type = "join";
		joinPkt.tag = sendTag;
		joinPkt.send(sock, predAdr, debug);

		InetSocketAddress sender = null;
		Packet replyPkt = new Packet();
		// wait for a "success" packet
		while (true) {
			try { sender = replyPkt.receive(sock, debug);
			} catch(Exception e) {
				System.err.println("In join(): fail to receive success packet");
				continue;
			}
			if (sender == null) {
				System.err.println("In join(): fail to receive success packet");
				continue;
			}
			if (!replyPkt.type.equals("success")) continue;
			else {
				handlePacket(replyPkt,sender);
				break;
			}
		}
	}
	
	/** Handle a join packet from a prospective DHT node.
	 *  @param p is the received join packet
	 *  @param succAdr is the socket address of the host that
	 *  sent the join packet (the new successor)
	 *
	 *  ++documentation
	 *  Split its hash range in half and 
	 *  give the top half of the range to its new successor.
	 *  Send a "success" packet to its new successor 
	 *  with succInfo, predInfo and hashRange fields.
	 *  Send an "update" packet to its original successor 
	 *  to update its predecessor.
	 *  Set new successor and new hashRange.
	 *  Add its succInfo into its routing table.
	 *  Send a series of "transfer" packets to new successor
	 *  and remove these pairs from local map.
	 */
	public static void handleJoin(Packet p, InetSocketAddress succAdr) {
		// ++code
		// send a "success" packet to the joining server (new successor)
		// (fields: type, tag, hashRange, predInfo, succInfo)
		Packet replyPkt = new Packet();
		replyPkt.type = "success";
		replyPkt.tag = sendTag;
		// assign top half of hashRange to the packet
		int mid = hashRange.left + (hashRange.right - hashRange.left) / 2;
		int top = hashRange.right;
		replyPkt.hashRange = new Pair<Integer,Integer>(mid, top);
		replyPkt.succInfo = succInfo;
		replyPkt.predInfo = myInfo;
		replyPkt.send(sock, succAdr, debug);

		// send an "update" packet to its original successor 
		// (fields: type, tag, predInfo, senderInfo)
		Packet updatePkt = new Packet();
		updatePkt.type = "update";
		updatePkt.tag = sendTag;
		updatePkt.senderInfo = myInfo;
		Pair<InetSocketAddress,Integer> joinerInfo = 
			new Pair<InetSocketAddress,Integer>(succAdr, mid);
		updatePkt.predInfo = joinerInfo;
		updatePkt.send(sock, succInfo.left, debug);

		// set new successor and new hashRange
		// add its new successor into its routing table
		succInfo = joinerInfo;
		addRoute(succInfo);
		hashRange.right = mid;

		// transfer (key,value) pair to the joining server 
		ArrayList<String> deletedKeys = new ArrayList<String>();
		for (String key : map.keySet()) {	
			if(hashit(key) >= mid) {
				// send a "transfer" packet to its new successor
				// (fields: type, tag, key, val)
				Packet xferPkt = new Packet();
				xferPkt.type = "transfer";
				xferPkt.tag = sendTag;
				xferPkt.key = key;
				xferPkt.val = map.get(key);
				xferPkt.send(sock, succAdr, debug);
				// prepare to remove
				deletedKeys.add(key);
			}
		}
		// remove pairs
		for (String key : deletedKeys)
			map.remove(key);
	}
	
	/** Handle a get packet.
	 *  @param p is a get packet
	 *  @param senderAdr is the the socket address of the sender
	 *
	 *  ++documentation
	 *  if cacheOn, find key from cache first
	 *  Hash the key
	 *  From the client:
	 * 		Not responsible -> forward the packet (+ clientAdr & relayAdr)
	 *  	Responsible -> respond directly to clientAdr
	 *  From another server:
	 *   	Not responsible -> forward the packet
	 *   	Responsible -> perform operation, convert packet to a response packet
	 * 					   send it to relay server (using relayAdr) (+senderInfo)
	 *      SenderInfo is used to establish shortcut routes
	 * 	Get operation:
	 * 		constains key -> get the value of the key: map.get(key)
	 * 						 send "succees" packet to relayAdr (+val,senderInfo)
	 *		no key  	  -> send "no match" packet to relayAdr (senderInfo)
	 */
	public static void handleGet(Packet p, InetSocketAddress senderAdr) {
		// ++code
		// if the cache has the pair, return it directly to the client
		if(cacheOn && cache.containsKey(p.key)){
			p.type = "success";
			p.val = cache.get(p.key);
			p.send(sock, senderAdr, debug);
			return;
		}
		InetSocketAddress replyAdr;
		int hash = hashit(p.key);
		int left = hashRange.left.intValue();
		int right = hashRange.right.intValue();

		if (left <= hash && hash <= right) {
			// respond to request using map
			// from other server
			if (p.relayAdr != null) {
				replyAdr = p.relayAdr;
				p.senderInfo = myInfo;
			} 
			// from the client
			else {
				replyAdr = senderAdr;
			}
			if (map.containsKey(p.key)) {
				p.type = "success"; p.val = map.get(p.key);
			} else {
				p.type = "no match";
			}
			p.send(sock,replyAdr,debug);
		} else {
			// forward around DHT
			if (p.relayAdr == null) {
				p.relayAdr = myAdr; p.clientAdr = senderAdr;
			}
			forward(p,hash);
		}
	}
	
	/** Handle a put packet.
	 *  @param p is a put packet
	 *  @param senderAdr is the the socket address of the sender
	 *  
	 *  ++documentation
	 *  if cacheOn, remove key from cache first
	 *  Hash the key
	 *  From the client:
	 * 		Not responsible -> forward the packet (+ clientAdr & relayAdr)
	 *  	Responsible -> respond directly to clientAdr
	 *  From another server:
	 *   	Not responsible -> forward the packet
	 *   	Responsible -> perform operation, convert packet to a response packet
	 * 					   send it to relay server (using relayAdr) (+senderInfo)
	 *      SenderInfo is used to establish shortcut routes
	 * 	Put operation:
	 * 		constains key -> replace the value of the key: map.put(key)
	 * 						 if val is omitted, remove the key
	 *		no key  	  -> set the value of the key: map.put(key)
	 *		send "success" packet to relayAdr (+senderInfo)
	 */
	public static void handlePut(Packet p, InetSocketAddress senderAdr) {
		// ++code
		// if the cache has the pair, remove it first
		if(cacheOn && cache.containsKey(p.key)){
			cache.remove(p.key);
		}
		InetSocketAddress replyAdr;
		int hash = hashit(p.key);
		int left = hashRange.left.intValue();
		int right = hashRange.right.intValue();

		if(left <= hash && hash <= right){
			p.type = "success";
			if (p.val != null) {
				map.put(p.key, p.val);
			} else {
				map.remove(p.key); // if val is omitted
			}
			// from other server
			if(p.relayAdr != null) {
				replyAdr = p.relayAdr;
				p.senderInfo = myInfo;
			}
			// from the client
			else {
				replyAdr = senderAdr;
			}
			p.send(sock, replyAdr, debug);
		} else {
			// forward around DHT
			if (p.relayAdr == null){
				p.relayAdr = myAdr; p.clientAdr = senderAdr;
			}
			forward(p,hash);
		}
	}

	/** Handle a transfer packet.
	 *  @param p is a transfer packet
	 *  @param senderAdr is the the address (ip:port) of the sender
	 *  
	 *  ++documentation
	 *	Update its local map (No reply needed).
	 */
	public static void handleXfer(Packet p, InetSocketAddress senderAdr) {
		// ++code
		map.put(p.key, p.val);
	}
	
	/** Handle a reply packet.
	 *  @param p is a reply packet, more specifically, a packet of type
	 *  "success", "failure" or "no match"
	 *  @param senderAdr is the the address (ip:port) of the sender
	 *  
	 *  ++documentation
	 *	1. For joining
	 *	update hashRange, succInfo, predInfo and myInfo
	 *  Add its successor into its routing table
	 *  2. For get/put
	 *  This server is assumed to be a reply server
	 *  Forward the packet on to the client using clientAdr
	 *  (remove clientAdr, repayAdr, senderInfo first).
	 *  Add a shortcut route to its routing table using senderInfo.
	 *  If "success" packet is received, store the pair in the cache.
	 */
	public static void handleReply(Packet p, InetSocketAddress senderAdr) {
		// ++code
		// receive "success" packet for joining 
		if (p.hashRange != null) {
			hashRange = p.hashRange;
			succInfo = p.succInfo;
			predInfo = p.predInfo;
			myInfo = new Pair<InetSocketAddress,Integer>(myAdr, hashRange.left);
			addRoute(succInfo);
			return;
		}

		// receive reply packet for get/put
		InetSocketAddress client = p.clientAdr; // store the client address
		// remove clientAdr, relayAdr, senderInfo
		p.clientAdr = null;
		p.relayAdr = null;
		p.senderInfo = null;
		// if packet type is "success", store in the cache
		if (cacheOn && p.type.equals("success") && p.val != null) {
			cache.put(p.key, p.val);
		}
		// send the packet
		p.send(sock, client, debug);
	}
	
	/** Handle packets received from clients or other servers
	 *  @param p is a packet
	 *  @param senderAdr is the address (ip:port) of the sender
	 */
	public static void handlePacket(Packet p, InetSocketAddress senderAdr) {
		if (p.senderInfo != null & !p.type.equals("leave"))
			addRoute(p.senderInfo);
		if (p.type.equals("get")) {
			handleGet(p,senderAdr);
		} else if (p.type.equals("put")) {
			handlePut(p, senderAdr);
		} else if (p.type.equals("transfer")) {
			handleXfer(p, senderAdr);
		} else if (p.type.equals("success") ||
			   p.type.equals("no match") ||
		     	   p.type.equals("failure")) {
			handleReply(p, senderAdr);
		} else if (p.type.equals("join")) {
			handleJoin(p, senderAdr);
		} else if (p.type.equals("update")){
			handleUpdate(p, senderAdr);
		} else if (p.type.equals("leave")){
			handleLeave(p, senderAdr);
		}
	}
	
	/** Add an entry to the route tabe.
	 *  @param newRoute is a pair (addr,hash) where addr is the socket
	 *  address for some server and hash is the first hash in that
	 *  server's range
	 *
	 *  If the number of entries in the table exceeds the max
	 *  number allowed, the first entry that does not refer to
	 *  the successor of this server, is removed.
	 *  If debug is true and the set of stored routes does change,
	 *  print the string "rteTbl=" + rteTbl. (IMPORTANT)
	 */
	public static void addRoute(Pair<InetSocketAddress,Integer> newRoute){
		// ++code
		if (newRoute.equals(myInfo)) return;
		// assume that succInfo is the first element in the routing table
		// if exceeds the max number allowed
		if (rteTbl.size() == numRoutes) {
			// if new route is succinfo, add it to the first position
			if (newRoute.equals(succInfo)) {
				rteTbl.remove(0);
				rteTbl.add(0, newRoute);
			} else {
				// if number of routes is 1, do nothing
				if (numRoutes == 1) return;
				// replace the first entry that is not succInfo
				rteTbl.remove(1);
				rteTbl.add(newRoute);
			}
		}
		else {
			if (newRoute.equals(succInfo)) 
				rteTbl.add(0, newRoute);
			else
				rteTbl.add(newRoute);
		}	

		if (debug) {
			System.out.println("rteTbl=" + rteTbl);
			System.out.println(); System.out.flush();
		}
	}

	/** Remove an entry from the route tabe.
	 *  @param rmRoute is the route information for some server 
	 *  need to be removed from route table
	 *
	 *  If the route information exists in current entries, remove it.
	 *	Otherwise, do nothing.
	 *  If debug is true and the set of stored routes does change,
	 *  print the string "rteTbl=" + rteTbl. (IMPORTANT)
	 */
	public static void removeRoute(Pair<InetSocketAddress,Integer> rmRoute){
		// ++code
		if (rteTbl.contains(rmRoute)) {
			rteTbl.remove(rmRoute);
			if (debug) {
				System.out.println("rteTbl=" + rteTbl);
				System.out.println(); System.out.flush();
			}
		}
	}


	/** Forward a packet using the local routing table.
	 *  @param p is a packet to be forwarded
	 *  @param hash is the hash of the packet's key field
	 *
	 *  This method selects a server from its route table that is
	 *  "closest" to the target of this packet (based on hash).
	 *  If firstHash is the first hash in a server's range, then
	 *  we seek to minimize the difference hash-firstHash, where
	 *  the difference is interpreted modulo the range of hash values.
	 *  IMPORTANT POINT - handle "wrap-around" correctly. 
	 *  Once a server is selected, p is sent to that server.
	 */
	public static void forward(Packet p, int hash) {
		// ++code
		InetSocketAddress forwardAdr = null;
		int min = Integer.MAX_VALUE;
		// find closest route from the routing table
		for (Pair<InetSocketAddress,Integer> route : rteTbl) {
			int difference = hash - route.right;
			if (difference < 0)
				difference += Integer.MAX_VALUE; // handle "wrap around"
			// find the minimum difference
			if (difference < min) {
				min = difference;
				forwardAdr = route.left;
			}
		}
		// forward the packet
		p.send(sock, forwardAdr, debug);
	}
}
