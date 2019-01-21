from libcloud.compute.ssh import ParamikoSSHClient, SSHCommandTimeoutError
from libcloud.utils.py3 import StringIO

import time


class CustomSSHClient(ParamikoSSHClient):
    """
    Adding functionalities to original libcloud ParamikoSSHClient
    """
    def put(self, localpath, remotepath):
        """Put a remote file

        :param localpath: local path where to find the file
        :param remotepath: remote path where to store the file
        :return: when the file is uploaded
        """
        sftp_client = self.client.open_sftp()
        sftp_client.put(localpath, remotepath)
    def get(self, localpath, remotepath):
        """Get a remote file

        :param localpath: local path where to store the file
        :param remotepath: remote path where to find the file
        :return: when file is downloaded
        """
        print("Remote: "+remotepath+ " Local: "+localpath)

        sftp = self.client.open_sftp()
        sftp.get(remotepath, localpath)

    def listdir(self, remotepath):
        """List files in the path

        :param remotepath: remote path
        :return: list of files in the remote path
        """
        sftp = self.client.open_sftp()
        return sftp.listdir(remotepath)

    def run(self, cmd, timeout=None):
        """
        Note: This function is based on paramiko's exec_command()
        method.

        :param timeout: How long to wait (in seconds) for the command to
                        finish (optional).
        :type timeout: ``float``
        """
        extra = {'_cmd': cmd}
        self.logger.debug('Executing command', extra=extra)

        # Use the system default buffer size
        bufsize = -1

        transport = self.client.get_transport()
        chan = transport.open_session()

        start_time = time.time()
        chan.exec_command(cmd)

        stdout = StringIO()
        stderr = StringIO()

        # Create a stdin file and immediately close it to prevent any
        # interactive script from hanging the process.
        stdin = chan.makefile('wb', bufsize)
        stdin.close()

        # Receive all the output
        # Note #1: This is used instead of chan.makefile approach to prevent
        # buffering issues and hanging if the executed command produces a lot
        # of output.
        #
        # Note #2: If you are going to remove "ready" checks inside the loop
        # you are going to have a bad time. Trying to consume from a channel
        # which is not ready will block for indefinitely.
        exit_status_ready = chan.exit_status_ready()

        if exit_status_ready:
            # It's possible that some data is already available when exit
            # status is ready
            stdout.write(self._consume_stdout(chan).getvalue())
            stderr.write(self._consume_stderr(chan).getvalue())

        while not exit_status_ready:
            current_time = time.time()
            elapsed_time = (current_time - start_time)

            if timeout and (elapsed_time > timeout):
                # TODO: Is this the right way to clean up?
                chan.close()

                raise SSHCommandTimeoutError(cmd=cmd, timeout=timeout)

            stdout.write(self._consume_stdout(chan).getvalue())
            stderr.write(self._consume_stderr(chan).getvalue())

            # We need to check the exist status here, because the command could
            # print some output and exit during this sleep below.
            exit_status_ready = chan.exit_status_ready()

            if exit_status_ready:
                break

            # Short sleep to prevent busy waiting
            time.sleep(self.SLEEP_DELAY)

        # Receive the exit status code of the command we ran.
        status = chan.recv_exit_status()

        stdout = stdout.getvalue()
        stderr = stderr.getvalue()

        extra = {'_status': status, '_stdout': stdout, '_stderr': stderr}
        self.logger.debug('Command finished', extra=extra)

        # print("CMD: {}".format(cmd))
        # print("OUT: {} ERR: {} STATUS: {}".format(stdout, stderr, status))


        return [stdout, stderr, status]



def sshclient_from_node(node, ssh_key_file,  host_key_file='~/.ssh/known_hosts', user_name='root', ssh_pwd=None):
    """
    Creates an ssh client to connect to the node

    :param node: node to which you want to connect
    :param ssh_key_file: ssh key file
    :param host_key_file: host key file
    :param user_name: username
    :param ssh_pwd: password
    :return: a connected ssh client
    """
    client =  CustomSSHClient(hostname=node.public_ips[0],
                               port=22,
                               username=user_name,
                               password=ssh_pwd,
                               key_files=ssh_key_file)
    client.connect()
    return client


def sshclient_from_ip(ip, ssh_key_file,  host_key_file='~/.ssh/known_hosts', user_name='root', ssh_pwd=None):
    """
    Creates an ssh client to connect to the node

    :param ip: ip to which you want to connect
    :param ssh_key_file: ssh key file
    :param host_key_file: host key file
    :param user_name: username
    :param ssh_pwd: password
    :return: a connected ssh client
    """
    client = CustomSSHClient(hostname=ip,
                             port=22,
                             username=user_name,
                             password=ssh_pwd,
                             key_files=ssh_key_file)
    client.connect()
    return client
