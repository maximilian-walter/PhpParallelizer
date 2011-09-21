<?php

  /**
   * PhpParallelizer
   * Runs multiple jobs in parallel
   *
   * USE AT YOUR OWN RISK!
   *
   * Example-Usage:
   *
   * <code>
   * require('php_parallelizer.php');
   *
   * $exampleClass = new ExampleClass;
   *
   * $phpParallelizer = new PhpParallelizer;
   * $phpParallelizer->addJob('exampleFunction', array('param1' => 'value1', 'param2' => 'value2'));
   * $phpParallelizer->addJob(array('ExampleClass', 'staticMethod'), array('param1' => 'value1', 'param2' => 'value2'));
   * $phpParallelizer->addJob(array($exampleClass, 'nonStaticMethod'), array('param1' => 'value1', 'param2' => 'value2'));
   * $phpParallelizer->run();
   * $log = $phpParallelizer->getLog();
   * </code>
   *
   * @copyright Maximilian Walter
   * @licence GNU General Public License version 3 (GPLv3) (http://www.opensource.org/licenses/gpl-3.0.html)
   * @since 09/20/2011
   * @version $Id$
   * @package php_parallelizer
   */

  /**
   * PhpParallelizer
   *
   * @author Maximilian Walter
   * @since 09/20/2011
   * @package php_parallelizer
   */
  class PhpParallelizer {

    /**
     * Array with all Jobs
     *
     * @var array
     */
    protected $jobs = array();

    /**
     * Array with current running jobs
     *
     * @var array
     */
    protected $currentJobs = array();

    /**
     * Array with unprocessed signals
     *
     * @var array
     */
    protected $signalQueue = array();

    /**
     * Logging
     *
     * @var array
     */
    protected $log = array();

    /**
     * Constructor - Checks system-requirements
     *
     * @author Maximilian Walter
     * @since 09/20/2011
     * @version 1.0
     * @return void
     * @throws Exception
     */
    public function __construct() {
      if (!function_exists('pcntl_fork')) {
        throw new Exception('PCNTL functions not available on this PHP installation');
      }
      
      declare(ticks = 1);
      pcntl_signal(SIGCHLD, array($this, 'signalHandler'));
    }

    /**
     * Runs all Jobs in parallel mode
     *
     * @author Maximilian Walter
     * @since 09/20/2011
     * @version 1.0
     * @return void
     * @throws Exception
     */
    public function run() {
      if (empty($this->jobs)) {
        return;
      }

      foreach ($this->jobs as $jobId => $job) {
        $pid = pcntl_fork();

        if (-1 == $pid) {
          throw new Exception('Fork failed!');
        }
        elseif (0 == $pid) {
          $result = call_user_func_array($job['function'], $job['params']);
          $code = (false === $result) ? 1 : 0;
          exit($code);
        }
        else {
          $this->currentJobs[$pid] = $jobId;
          
          if (isset($this->signalQueue[$pid])){
            $this->signalHandler(SIGCHLD, $pid, $this->signalQueue[$pid]);
            unset($this->signalQueue[$pid]);
          }
        }
      }

      while (count($this->currentJobs)) {
        sleep(1);
      }
      
      $this->jobs = array();
    }

    /**
     * Adds new Job to the queue
     *
     * @author Maximilian Walter
     * @since 09/20/2011
     * @version 1.0
     * @param mixed $function Callback
     * @param array $params = array() Parameters to be passed to the function, as indexed array
     * @return bool
     */
    public function addJob($function, $params = array()) {
      if (is_callable($function)) {
        $jobId = uniqid();
        $this->jobs[$jobId] = array(
          'function' => $function,
          'params' => $params,
        );
        
        return true;
      }
      else {
        return false;
      }
    }

    /**
     * Signal handler
     *
     * @author Maximilian Walter
     * @since 09/21/2011
     * @version 1.0
     * @param int $signo Signal
     * @param int $pid = null PID
     * @param int $status = null Status
     * @return true
     */
    protected function signalHandler($signo, $pid = null, $status = null) {
      if (!$pid) {
        $pid = pcntl_waitpid(-1, $status, WNOHANG);
      }

      while ($pid > 0){
        if ($pid && isset($this->currentJobs[$pid])){
          $exitCode = pcntl_wexitstatus($status);
          $this->setLog("{$pid} exited with status {$exitCode}");
          unset($this->currentJobs[$pid]);
        }
        elseif ($pid) {
          $this->signalQueue[$pid] = $status;
        }
        $pid = pcntl_waitpid(-1, $status, WNOHANG);
      }
      return true;
    }

    /**
     * Writes a log-entry
     *
     * @author Maximilian Walter
     * @since 09/21/2011
     * @version 1.0
     * @param string $message Message
     * @return void
     */
    protected function setLog($message) {
      if (!empty($message)) {
        $this->log[] = $message;
      }
    }

    /**
     * Returns all log-entries
     *
     * @author Maximilian Walter
     * @since 09/21/2011
     * @version 1.0
     * @return array
     */
    public function getLog() {
      return $this->log;
    }
  }