<?php
/**
 * HandlerSocket sharded client
 *
 * Add 'session_template' to r_indexes and w_indexes!
 *
 * Usage sample:

$hs_params = array(
'r_indexes' => array(
'session_template' => array('db_name' => 'toolbar%d', 'table_name' => 'sess%d', 'index_name' => 'PRIMARY', 'fields' => array('bucket', 'hash', 'data')),
),
'w_indexes' => array(
'session_template' => array('db_name' => 'toolbar%d', 'table_name' => 'sess%d', 'index_name' => 'PRIMARY', 'fields' => array('bucket', 'hash', 'ts', 'data')),
),
'host' => 'bdasessions1.mlan', 'r_port' => 9998, 'w_port' => 9999, 'persistent' => false, 'conn_timeout' => 1, 'r_conn_timeout' => 1, 'w_conn_timeout' => 3, 'retries' => 1
);
$HSClinet = new HandlerSocket\Client($hs_params);
$HSClinet->delete(1, 99);
$HSClinet->insert(1, $values, 99);
$HSClinet->close();
 */


namespace HandlerSocket;


class SessionClient extends Client
{

	const SGL_IDX_NUM_R = 90; //we always use single index num & always open it before sending any request
	const SGL_IDX_NUM_W = 91;


	/**
	 * Connect to R or W HS socket
	 *
	 * @param const $mode
	 * @throws ConnectionException
	 */
	function connect ( $mode )
	{
		parent::connect( $mode );
		$this->persistent = false; //force open index every time
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
		#under 10000 -> original scheme, single index also goes here
		if ( $index_num < 10000 )
			return parent::get_index_params( $mode, $index_num );

		$indexes = $mode == self::R_MODE ? $this->r_indexes : $this->w_indexes;

		if ( empty( $indexes['session_template'] ) )
			throw new IndexNotFoundException( "Index not found: $index_num", 5 );

		#10000—19999 -> R, 20000—29999 -> W
		$num = $index_num - ( $mode == self::R_MODE ? 10000 : 20000 );
		if ( $num > 9999 )
			throw new IndexNotFoundException( "Index not found: $index_num", 5 );

		$db_num                     = floor( $num / 100 );
		$tbl_num                    = floor( $num % 100 );
		$index_params               = $indexes['session_template'];
		$index_params['db_name']    = sprintf( $index_params['db_name'], $db_num );
		$index_params['table_name'] = sprintf( $index_params['table_name'], $tbl_num );

		return $index_params;
	}

	/**
	 * Get entry
	 *
	 * @param string|array $id
	 * @param int          $index_num
	 * @throws CommunicationException
	 * @return array|bool
	 */
	function select ( $id, $index_num )
	{
		#under 10000 -> original scheme
		if ( $index_num >= 10000 ) {
			#substitude index
			$this->r_indexes[self::SGL_IDX_NUM_R] = $this->get_index_params( self::R_MODE, $index_num );
			$index_num                            = self::SGL_IDX_NUM_R;
			unset( $this->open_indices[self::R_MODE][$index_num] );
		}

		return parent::select( $id, $index_num );
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
		#under 10000 -> original scheme
		if ( $index_num >= 10000 ) {
			#substitude index
			$this->w_indexes[self::SGL_IDX_NUM_W] = $this->get_index_params( self::W_MODE, $index_num );
			$index_num                            = self::SGL_IDX_NUM_W;
			unset( $this->open_indices[self::W_MODE][$index_num] );
		}

		return parent::insert( $id, $values, $index_num, $autoinc_mode );
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
		#under 10000 -> original scheme
		if ( $index_num >= 10000 ) {
			#substitude index
			$this->w_indexes[self::SGL_IDX_NUM_W] = $this->get_index_params( self::W_MODE, $index_num );
			$index_num                            = self::SGL_IDX_NUM_W;
			unset( $this->open_indices[self::W_MODE][$index_num] );
		}

		return parent::update( $id, $values, $index_num );
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
		#under 10000 -> original scheme
		if ( $index_num >= 10000 ) {
			#substitude index
			$this->w_indexes[self::SGL_IDX_NUM_W] = $this->get_index_params( self::W_MODE, $index_num );
			$index_num                            = self::SGL_IDX_NUM_W;
			unset( $this->open_indices[self::W_MODE][$index_num] );
		}

		return parent::delete( $id, $index_num );
	}
}
