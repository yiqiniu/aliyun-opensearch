<?php


namespace yiqiniu\sdk_search;


use Exception;
use OpenSearch\Client\OpenSearchClient;
use OpenSearch\Client\SearchClient;
use OpenSearch\Util\SearchParamsBuilder;


class OpenSearch
{

    private static $_instance = null;

    private $_debug = false;
    private $config = [
        'accessKeyId' => '',
        'secret' => '',
        'in_endPoint' => '',
        'out_endPoint' => '',
        'appName' => '',
        'suggestName' => '',
        'queryProcessor' => '',
    ];
    private $_in = true;

    /**
     * OpenSearch constructor.
     */
    private function __construct($config, $in, $debug)
    {
        if (!empty($config)) {
            $this->config = array_merge($this->config, $config);
        }
        $this->_debug = $debug;
        $this->_in = $in;
    }


    public static function getInstance($config, $in = true, $debug = false)
    {
        if (self::$_instance === null) {
            self::$_instance = new static($config, $in, $debug);
        }
        return self::$_instance;
    }

    /**
     * 获取查询客户端
     * @param bool $in
     * @return OpenSearchClient
     * @throws Exception
     */
    private function getClient()
    {

        $endPoint = $this->_in ? $this->config['in_endPoint'] : $this->config['out_endPoint'];
        if (empty($this->config['accessKeyId']) || empty($this->config['secret']) || empty($endPoint || empty($this->config['appName']))) {
            throw new Exception('开发搜索的参数未配置', 400);
        }
        $options = array('debug' => $this->_debug);
        return new OpenSearchClient(
            $this->config['accessKeyId'],
            $this->config['secret'],
            $endPoint,
            $options
        );
    }

    /**
     * 执行搜索动作
     * @param       $keyword
     * @param array $option
     * @return array|mixed
     * @throws Exception
     */
    private function searchAction($keyword, $option = [])
    {
        if (empty($keyword)) {
            return [];
        }
        try {
            $client = $this->getClient();

            $searchClient = new SearchClient($client);

            $params = new SearchParamsBuilder();
            $params->setAppName($this->config['appName']);
            if (isset($option['page_size'])) {
                $params->setHits($option['page_size']);
            } else {
                $params->setHits(100);
            }
            if (isset($option['page'], $option['page_size'])) {
                $params->setStart($option['page'] * $option['page_size']);
            }
            $params->setQuery("default:'$keyword'");
            $params->setFormat('json');
            if (!empty($this->config['queryProcessor'])) {
                $params->addQueryProcessor($this->config['queryProcessor']);
            }
            //查询条件
            //$params->setFilter('sh=1');
            if (!empty($option['where'])) {
                if (is_array($option['where'])) {
                    $wheres = array_map(function ($item) {
                        return implode('', $item);
                    }, $option['where']);
                    $option['where'] = implode(' AND ', $wheres);
                }
                $params->setFilter(str_replace('<>', '!=', $option['where']));
            }
            // 返回字段
            //$params->setFetchFields(['id','title','classid']);
            if (!empty($option['fields'])) {
                if (is_array($option['fields'])) {
                    $params->setFetchFields($option['fields']);
                } else {
                    $params->setFetchFields(explode(',', $option['fields']));
                }
            }
            // 添加排序
            if (!empty($option['order'])) {
                $orders = explode(',', $option['order']);
                foreach ($orders as $item) {
                    if (strpos($item, ' ') !== false) {
                        [$field, $order] = explode(' ', $item);
                        $params->addSort($field, SearchParamsBuilder::SORT_DECREASE);
                    } else {
                        $params->addSort($item, SearchParamsBuilder::SORT_INCREASE);
                    }
                }

            }
            //$params->addSort('id', SearchParamsBuilder::SORT_DECREASE);
            //  $build = $params->build();
            $ret = $searchClient->execute($params->build())->result;

            return json_decode($ret, true);
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * 按页返回
     * @param string $keyword 关键字
     * @param array  $option
     * @return array
     * @throws Exception
     */
    public function search_page($keyword, $option = [])
    {
        try {
            $json = $this->searchAction($keyword, $option);
            if (isset($json['result'], $json['result']['items'])) {

                if(($json['result']['total'] <=$option['page_size']) || $json['result']['total']<1){
                    $list['hasmore'] = false;
                }else{
                    $list['hasmore'] = $json['result']['num'] == $option['page_size'];
                }
                $list['list'] = $json['result']['items'];
                return $list;

            }
            return ['hasmore' => false, 'list' => []];
        } catch (Exception $e) {
            throw  $e;
        }
    }

    /**
     * 搜索前100个
     * 通过pagesize来设置
     * @param       $keyword
     * @param array $option
     * @return array|mixed
     * @throws Exception
     */
    public function search_list($keyword, $option = [])
    {
        try {
            if (!isset($option['page_size'])) {
                $option['page_size'] = 100;
                $option['page'] = 0;
            }
            $json = $this->searchAction($keyword, $option);
            if (isset($json['result'], $json['result']['items'])) {
                return $json['result']['items'];
            }
            return [];
        } catch (Exception $e) {
            throw  $e;
        }
    }

    /**
     * 获取查询后原始数据
     * @param       $keyword
     * @param array $option
     * @return array|mixed
     * @throws Exception
     */
    public function search($keyword, $option = [])
    {
        try {
            return $this->searchAction($keyword, $option);

        } catch (Exception $e) {
            throw  $e;
        }
    }
}