#from threading import Lock
from queue import Queue, Empty
import socket
import ssl
from urllib.parse import urlparse

class ReusablePool:
	"""
	Manage Reusable objects for use by Client objects.
	"""

	def __init__(self, pool_size):
		#could replace array with queue; and then remove LOck()
		self._reusables = Queue()
		for _ in range(pool_size):
			self._reusables.put(ReusableConnection())
		
	def get_connection(self):
		try:
			return(self._reusables.get(timeout=60))
		except Empty:
			return None
	
	def release_connection(self, reusable_connection):
		try:	
			if (reusable_connection.is_broken()):
				self._reusables.put(ReusableConnection())
			else:
				self._reusables.put(reusable_connection)

		except Exception as e:
			print('release_connection error : '+ e)
			pass

	def end_pool(self):
		while not self._reusables.empty():
			try:
				conn = self._reusables.get(timeout = 5)
				conn.close_connection()
			except Empty:
				pass

class ReusableConnection:
	def __init__(self):
		self._broken = False

		host="www.medium.com"
		port = 443
		
		# Set up a TCP/IP socket
		self._socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

		# Connect as client to a selected serve on ssl port
		self._socket.connect((host, port))

		# ssl for https
		self._wrappedSocket = ssl.wrap_socket(self._socket, ssl_version=ssl.PROTOCOL_TLSv1)

	def get_data(self, some_medium_url):
		try:
			html_data = ''

			url = urlparse(some_medium_url)
			resource = url.path or '/'

			self._wrappedSocket.sendall("GET " + resource + " HTTP/1.1\r\nHost: medium.com\r\n\r\n")

			while True:
				#print "here"
				#decode utf-8
				response = self._wrappedSocket.recv(1024).decode("utf8")
				if response == "": break
				html_data += response

			return html_data

		except Exception as e:
			self._broken = True
			raise e

	def close_connection(self):
		self._wrappedSocket.close()
		self._broken = True

	def is_broken(self):
		return self._broken


def main():
	reusable_pool = ReusablePool(2)
	reusable1 = reusable_pool.get_connection()
	reusable2 = reusable_pool.get_connection()
	#reusable3 = reusable_pool.get_connection() # This will be None as size is 2 and I am trying to get a third.

	res = reusable1.get_data("https://medium.com/topic/technology")
## to be written after finaly ....taaki koi bhi exception vagera aaye toh finally connection release ho jaaye	
	reusable_pool.release_connection(reusable1) # Always release a connection after you have fetched the html. Then only it will be available for others to use.
	print("2")
	res = reusable2.get_data("https://medium.com/topic/technology")
	print(res)

	

	reusable3 = reusable_pool.get_connection()
	res = reusable1.get_data("https://medium.com/m/signin?operation=login&redirect=https%3A%2F%2Fmedium.com%2Ftopic%2Feditors-picks")


	reusable_pool.end_pool()



if __name__ == "__main__":
	main()