<?php
/**
 * HandlerSocket communication framework
 *
 * Note I'm using mb_orig_* functions. If you don't use mbstring extension, replace mb_orig_* with general functions.
 *
 * Usage sample:

$hs_params = array(
'r_indexes' => array(
98 => array('db_name' => 'test', 'table_name' => TBLNM, 'index_name' => 'PRIMARY', 'fields' => array('key', 'value')),
),
'w_indexes' => array(
99 => array('db_name' => 'test', 'table_name' => TBLNM, 'index_name' => 'PRIMARY', 'fields' => array('key', 'value')),
),
'host' => 'bdasessions1.mlan', 'r_port' => 9998, 'w_port' => 9999, 'persistent' => false, 'conn_timeout' => 1, 'r_conn_timeout' => 1, 'w_conn_timeout' => 3, 'retries' => 1
);
$HSClinet = new HandlerSocket\Client($hs_params);
$HSClinet->delete(1, 99);
$HSClinet->insert(1, $values, 99);
$HSClinet->close();
 */


namespace HandlerSocket;

class HSException extends \Exception
{
}

class ConnectionException extends HSException
{
}

class CommunicationException extends HSException
{
}

class IndexNotFoundException extends HSException
{
}

class KeyAlreadyExistsException extends HSException
{
}

class Client
{
	const R_MODE                = 'r';
	const W_MODE                = 'w';
	const OP_EQUAL              = '=';
	const OP_MORE_THAN          = '>';
	const OP_MORE_OR_EQUAL_THAN = '>=';
	const OP_LESS_THAN          = '<';
	const OP_LESS_OR_EQUAL_THAN = '<=';
	const OPTION_MAP            = 'map';

	protected $r_conn;
	protected $w_conn;
	protected $open_indices = array();

	#params
	protected $host;
	protected $r_port;
	protected $w_port;
	protected $persistent;
	protected $conn_timeout;
	protected $r_conn_timeout;
	protected $w_conn_timeout;

	#0 => array('db_name' => 'bda', 'table_name' => 'sess', 'index_name' => 'PRIMARY', 'fields' => array('data')),
	protected $r_indexes;
	protected $w_indexes;

	function __construct ( array $params )
	{
		$params += array(
			'r_port'         => 9998,
			'w_port'         => 9999,
			'persistent'     => false,
			'conn_timeout'   => 2,
			'r_conn_timeout' => 2,
			'w_conn_timeout' => 3,
			'retries'        => 3,
			'r_indexes'      => array(),
			'w_indexes'      => array(),
		);
		$this->host           = $params['host'];
		$this->r_port         = $params['r_port'];
		$this->w_port         = $params['w_port'];
		$this->persistent     = $params['persistent'];
		$this->conn_timeout   = $params['conn_timeout'];
		$this->r_conn_timeout = $params['r_conn_timeout'];
		$this->w_conn_timeout = $params['w_conn_timeout'];
		$this->r_indexes      = $params['r_indexes'];
		$this->w_indexes      = $params['w_indexes'];
	}

	/**
	 * Connect to R or W HS socket
	 *
	 * @param string $mode
	 * @throws ConnectionException
	 */
	function connect ( $mode )
	{
		$conn = $mode == self::R_MODE ? $this->r_conn : $this->w_conn;
		if ( $conn ) return;

		$addr         = sprintf( '%s:%d', $this->host, $mode == self::R_MODE ? $this->r_port : $this->w_port );
		$connectFlags = STREAM_CLIENT_CONNECT;
		if ( $this->persistent ) $connectFlags |= STREAM_CLIENT_PERSISTENT;

		$conn = @stream_socket_client( 'tcp://' . $addr . '/', $errno, $errstr, $this->conn_timeout, $connectFlags );

		if ( !$conn ) {
			throw new ConnectionException( trim( $errstr ), $errno );
		}

		$io_timeout      = $mode == self::R_MODE ? $this->r_conn_timeout : $this->w_conn_timeout;
		$timeoutSeconds  = floor( $io_timeout );
		$timeoutUSeconds = ( $io_timeout - $timeoutSeconds ) * 1000000;
		stream_set_timeout( $conn, $timeoutSeconds, $timeoutUSeconds );

		if ( $mode == self::R_MODE ) {
			$this->r_conn = $conn;
		} else {
			$this->w_conn = $conn;
		}
	}

	/**
	 * Closes connection
	 * @param mixed $mode
	 */
	function close ( $mode = false )
	{
		if ( $mode == self::R_MODE || $mode === false ) {
			stream_socket_shutdown( $this->r_conn, STREAM_SHUT_RDWR );
			@fclose( $this->r_conn );
			$this->r_conn = null;
			unset( $this->open_indices[self::R_MODE] );
		}

		if ( $mode == self::W_MODE || $mode === false ) {
			stream_socket_shutdown( $this->w_conn, STREAM_SHUT_RDWR );
			@fclose( $this->w_conn );
			$this->w_conn = null;
			unset( $this->open_indices[self::W_MODE] );
		}
	}

	/**
	 * Encode string for HS protocol
	 *
	 * @param string $data
	 * @return string
	 */
	static function encode ( $data )
	{
		/*
		 * - A token is either NULL or an encoded string. Note that you need to
		 *   distinguish NULL from an empty string, as most DBMs does so.
		 * - Characters in the range [0x10 - 0xff] are not encoded.
		 * - A character in the range [0x00 - 0x0f] is prefixed by 0x01 and
		 *   shifted by 0x40. For example, 0x03 is encoded as 0x01 0x43.
		 * - NULL is expressed as a single NUL(0x00).
		 */

		if ( $data === null ) return "\x00";

		return preg_replace( '/[\x00-\x0f]/es', 'chr(0x01).chr(ord("$0")+0x40)', $data );
	}

	/**
	 * Decode string for HS protocol
	 * @param string $data
	 * @return string
	 */
	static function decode ( $data )
	{
		if ( "\x00" === $data ) return null;

		return preg_replace( '/[\x01]([\x40-\x4f])/es', 'chr(ord("$1")-0x40)', $data );
	}

	/**
	 * Open index
	 *
	 * @param string $mode
	 * @param int    $index_num
	 * @throws IndexNotFoundException, CommunicationException
	 */
	function open_index ( $mode, $index_num )
	{
		if ( isset( $this->open_indices[$mode][$index_num] ) ) {
			return;
		}
		$addr = sprintf( '%s:%d', $this->host, $mode == self::R_MODE ? $this->r_port : $this->w_port );

		$index_params = $this->get_index_params( $mode, $index_num );

		$cmd_open = array(
			'P', $index_num, $index_params['db_name'], $index_params['table_name'], $index_params['index_name'], implode( ',', $index_params['fields'] )
		); //open index
		if ( isset( $index_params['filter_fields'] ) ) {
			$cmd_open[] = $index_params['filter_fields'];
		}
		if ( !$this->send( $mode, $cmd_open ) ) {
			$this->close( $mode );
			throw new CommunicationException( 'Error sending cmd', 1 );
		}

		$r = $this->receive( $mode );
		if ( $r != array( '0', '1' ) ) {
			$this->close( $mode );
			throw new CommunicationException( "Failed opening index $index_num" . ( isset( $r[2] ) ? ", err: {$r[2]}" : '' ), 3 );
		}

		$this->open_indices[$mode][$index_num] = true;
	}

	/**
	 * Returns index params
	 *
	 * @throws IndexNotFoundException
	 * @param string $mode
	 * @param int    $index_num
	 * @return array
	 */
	protected function get_index_params ( $mode, $index_num )
	{
		$indexes = $mode == self::R_MODE ? $this->r_indexes : $this->w_indexes;
		if ( empty( $indexes[$index_num] ) ) {
			throw new IndexNotFoundException( "Index not found: $index_num", 5 );
		}

		$index_params = $indexes[$index_num];
		return $index_params;
	}

	/**
	 * @param int   $index_num
	 * @param       $op
	 * @param array $values
	 * @param int   $limit
	 * @param int   $offset
	 * @param array $filter
	 * @param array $options
	 * @return array|null
	 * @throws CommunicationException
	 * @throws HSException
	 */
	public function find ( $index_num, $op, array $values, $limit = 1, $offset = 0, array $filter = array(), array $options = array( self::OPTION_MAP => true ) )
	{
		$in_index  = -1;
		$in_values = array();
		foreach ( $values as $index => &$value ) {
			if ( is_array( $value ) ) {
				throw new HSException( 'IN not working yet', 1 );
				$in_index  = $index;
				$in_values = $value;
				$value     = '';
			}
		}
		$lim = array();
		if ( 1 != $limit or 0 != $offset ) {
			$lim = array( $limit, $offset );
		}
		$in_prefix = array();
		if ( $in_values ) {
			$in_prefix = array( '@', $in_index, count( $in_values ) );
		}
		if ( $filter ) {
			throw new HSException( 'FILTER not implemented yet', 2 );
		}
		$cmd = array_merge( array( $index_num, $op, count( $values ) ), $values, $lim, $in_prefix, $in_values, $filter );

		$this->connect( self::R_MODE );
		if ( !$this->persistent ) {
			$this->open_index( self::R_MODE, $index_num );
		}

		$addr         = sprintf( '%s:%d', $this->host, $this->r_port );
		$timer_params = array(
			'tags' => array(
				'group'  => 'hsc_find',
				'server' => $addr,
				'result' => 'OK',
			),
			'data' => array( 'msg' => sprintf( 'HandlerSocketClient find at %s', $addr ) ),
		);
		$try          = 0;
		while ( $try++ <= 1 ) {
			if ( !$this->send( self::R_MODE, $cmd ) ) {
				$this->close( self::R_MODE );
				throw new CommunicationException( 'Error sending cmd', 1 );
			}

			$r = $this->receive( self::R_MODE );

			//if we are in persistent mode, we try to read before and open index only when it's reported as closed
			if ( $this->persistent && array( '2', '1', 'stmtnum' ) == $r ) {
				$this->open_index( self::R_MODE, $index_num );
				continue;
			}

			//check we got proper response
			if ( 0 != $r[0] or count( $r ) < 2 ) {
				$this->close( self::R_MODE );
				throw new CommunicationException( 'Failed getting values from DB' . ( isset( $r[2] ) ? ', err: ' . $r[2] : '' ), 4 );
			}

			//everything is okay, return result
			$rows = array_chunk( array_slice( $r, 2 ), $r[1] );
			if ( !empty( $options[self::OPTION_MAP] ) ) {
				$rows_keys    = array();
				$index_params = $this->get_index_params( self::R_MODE, $index_num );
				for ( $i = 0, $n = count( $rows ); $i < $n; ++$i ) {
					$rows_keys[] = $index_params['fields'];
				}
				$rows = array_map( 'array_combine', $rows_keys, $rows );
			}
			return $rows;
		}
		return null;
	}

	/**
	 * Get entry
	 *
	 * @param string|array $id
	 * @param int          $index_num
	 * @return array|bool
	 * @throws CommunicationException
	 */
	public function select ( $id, $index_num )
	{
		$result = $this->find( $index_num, self::OP_EQUAL, (array)$id, 1, 0, array(), array( self::OPTION_MAP => false ) );

		if ( $result === null ) {
			return false;
		} elseif ( $result === array() ) {
			return array();
		} else {
			return $result[0];
		}
	}

	/**
	 * Insert entry
	 *
	 * @param string|array $id
	 * @param string|array $values
	 * @param int          $index_num
	 * @param bool         $autoinc_mode
	 * @throws KeyAlreadyExistsException, CommunicationException
	 * @return mixed
	 */
	function insert ( $id, $values, $index_num, $autoinc_mode = false )
	{
		#connect w
		$this->connect( self::W_MODE );

		#non-persistent conn-s: surely open index
		if ( !$this->persistent ) $this->open_index( self::W_MODE, $index_num );

		if ( $autoinc_mode ) $cmd_insrt = array_merge( array( $index_num, '+', sizeof( $values ) ), (array)$values ); //insert without key
		else {
			$id        = (array)$id;
			$cmd_insrt = array_merge( array( $index_num, '+', sizeof( $id ) + sizeof( $values ) ), $id, (array)$values ); //insert key
		}

		$addr = sprintf( '%s:%d', $this->host, $this->w_port );

		$try = 0;
		while ( $try <= 1 ) { //second try when opening index
			#try upd
			if ( !$this->send( self::W_MODE, $cmd_insrt ) ) {
				$this->close( self::W_MODE );
				throw new CommunicationException( 'Error sending cmd', 1 );
			}

			$r = $this->receive( self::W_MODE );

			if ( $this->persistent && $r == array( '2', '1', 'stmtnum' ) ) { //index not opened
				#open index & +1 attempt
				$this->open_index( self::W_MODE, $index_num );
				$try++;
				continue;
			}

			if ( array_slice( $r, 0, 2 ) == array( '1', '1' ) ) {
				throw new KeyAlreadyExistsException( 'Failed inserting values: key exists' . ( isset( $r[2] ) ? ", err: {$r[2]}" : '' ), 5 );
			}

			if ( array_slice( $r, 0, 2 ) != array( '0', '1' ) ) { // "0   1   {last_insert_id}" for autoincrement
				$this->close( self::W_MODE );
				throw new CommunicationException( 'Failed inserting values in DB (' . var_export( $r, 1 ) . ')' . ( isset( $r[2] ) ? ", err: {$r[2]}" : '' ), 4 );
			}

			if ( sizeof( $r ) == 3 ) {
				return $r[2]; //last_insert_id
			}

			return true;
		}

		return false; //impossible
	}

	/**
	 * Update entry
	 *
	 * @param string|array $id
	 * @param string|array $values
	 * @param int          $index_num
	 * @throws CommunicationException
	 * @return boolean|string true on success, false if no key found
	 */
	function update ( $id, $values, $index_num )
	{
		#connect w
		$this->connect( self::W_MODE );

		#non-persistent conn-s: surely open index
		if ( !$this->persistent ) $this->open_index( self::W_MODE, $index_num );

		$id      = (array)$id;
		$cmd_upd = array_merge( array( $index_num, self::OP_EQUAL, sizeof( $id ) ), $id, array( 1, 0, 'U' ), (array)$values ); //update key

		$addr = sprintf( '%s:%d', $this->host, $this->w_port );

		$try = 0;
		while ( $try <= 1 ) { //second try when opening index
			#try upd
			if ( !$this->send( self::W_MODE, $cmd_upd ) ) {
				$this->close( self::W_MODE );
				throw new CommunicationException( 'Error sending cmd', 1 );
			}

			$r = $this->receive( self::W_MODE );

			if ( $this->persistent && $r == array( '2', '1', 'stmtnum' ) ) { //index not opened
				#open index & +1 attempt
				$this->open_index( self::W_MODE, $index_num );
				$try++;
				continue;
			}

			if ( $r[0] != 0 || sizeof( $r ) < 3 ) {
				$this->close( self::W_MODE );
				throw new CommunicationException( 'Failed updating values in DB' . ( isset( $r[2] ) ? ", err: {$r[2]}" : '' ), 4 );
			}

			return (bool)$r[2];
		}

		return false; //impossible
	}

	/**
	 * Delete entry
	 *
	 * @param string|array $id
	 * @param int          $index_num
	 * @throws CommunicationException
	 * @return bool true on success, false if no key found
	 */
	function delete ( $id, $index_num )
	{
		#connect w
		$this->connect( self::W_MODE );

		#non-persistent conn-s: surely open index
		if ( !$this->persistent ) $this->open_index( self::W_MODE, $index_num );

		$id      = (array)$id;
		$cmd_upd = array_merge( array( $index_num, self::OP_EQUAL, sizeof( $id ) ), $id, array( 1, 0, 'D' ) ); //delete key

		$addr = sprintf( '%s:%d', $this->host, $this->w_port );

		$try = 0;
		while ( $try <= 1 ) { //second try when opening index
			#try upd
			if ( !$this->send( self::W_MODE, $cmd_upd ) ) {
				$this->close( self::W_MODE );
				throw new CommunicationException( 'Error sending cmd', 1 );
			}

			$r = $this->receive( self::W_MODE );

			if ( $this->persistent && $r == array( '2', '1', 'stmtnum' ) ) { //index not opened
				#open index & +1 attempt
				$this->open_index( self::W_MODE, $index_num );
				$try++;
				continue;
			}

			if ( $r[0] != 0 || sizeof( $r ) < 3 ) {
				$this->close( self::W_MODE );
				throw new CommunicationException( 'Failed deleting values from DB' . ( isset( $r[2] ) ? ", err: {$r[2]}" : '' ), 4 );
			}

			return (bool)$r[2];
		}

		return false; //impossible
	}

	/**
	 * Send cmd
	 *
	 * @param string $mode
	 * @param array  $cmd
	 * @return bool
	 */
	protected function send ( $mode, array $cmd )
	{
		$conn = $mode == self::R_MODE ? $this->r_conn : $this->w_conn;
		foreach ( $cmd as &$i ) {
			$i = self::encode( $i );
		}

		$data = implode( "\t", $cmd ) . "\n";

		try {
			while ( ( $len = mb_orig_strlen( $data ) ) > 0 ) {
				$wrtn = fwrite( $conn, $data );
				if ( $len === $wrtn ) return true;

				if ( $wrtn === false || $wrtn === 0 ) throw new CommunicationException( 'Socket timed out or got an error', 6 );

				$data = mb_orig_substr( $data, $wrtn );
			}
		}
		catch ( \Exception $e ) {
			$this->close( $mode );
			throw $e; //rethrow
		}

		return true;
	}

	/**
	 * Receive cmd
	 *
	 * @param string $mode
	 * @throws CommunicationException
	 * @return array
	 */
	protected function receive ( $mode )
	{
		$conn = $mode == self::R_MODE ? $this->r_conn : $this->w_conn;

		try {
			$str = fgets( $conn );

			if ( $str === false ) {
				throw new CommunicationException( 'Socket timed out or got an error', 6 );
			}

			if ( mb_orig_substr( $str, -1 ) !== "\n" ) throw new CommunicationException( 'Received malformed response: ' . rawurlencode( $str ), 2 );
		}
		catch ( \Exception $e ) {
			$this->close( $mode );
			throw $e; //rethrow
		}

		return array_map( 'self::decode', explode( "\t", mb_orig_substr( $str, 0, -1 ) ) );
	}
}
