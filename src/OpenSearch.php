<?php


namespace yiqiniu;


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
     * 开始搜索
     * @param string $keyword 关键字
     * @param array  $option
     * @return array
     * @throws Exception
     */
    public function search($keyword, $option = [])
    {
        if (empty($keyword)) {
            return [];
        }


        try {
            $client = $this->getClient();

            $searchClient = new SearchClient($client);

            $params = new SearchParamsBuilder();
            $params->setAppName($this->config['appName']);
            $params->setStart($option['page'] * $option['page_size']);
            $params->setHits($option['page_size']);
            $params->setQuery("default:'$keyword'");
            $params->setFormat('json');
            if (!empty($this->config['queryProcessor'])) {
                $params->addQueryProcessor($this->config['queryProcessor']);
            }
            //查询条件
            //$params->setFilter('sh=1');
            if (!empty($option['filter'])) {

                if (is_array($option['filter'])) {
                    $wheres = array_map(function ($item) {
                        return implode('', $item);
                    }, $option['filter']);
                    $option['filter'] = implode(' AND ', $wheres);
                }
                $option['filter'] = str_replace('<>', '!=', $option['filter']);
                $params->setFilter($option['filter']);
            }
            // 返回字段
            //$params->setFetchFields(['id','title','classid']);
            if (!empty($options['fetch_fields'])) {
                if (is_array($options['fetch_fields'])) {
                    $params->setFetchFields($option['fetch_fields']);
                } else {
                    $params->setFetchFields(explode(',', $option['fetch_fields']));
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

            $json = json_decode($ret, true);
            if (isset($json['result'], $json['result']['items'])) {

                if ($json['result']['total'] < 1) {
                    $list['hasmore'] = false;
                } else {
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
     * 只返回ID
     * @param $keyword
     * @param $option
     * @return array
     * @throws Exception
     */
    public function search_mini($keyword, $option)
    {
        $option['fetchFields'] = ['id', 'title'];
        $option['page_size'] = 300;
        $option['page'] = 0;
        return $this->search($keyword, $option);
    }

}