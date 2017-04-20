#!/usr/bin/env python

import rospy

def gossip():

    rospy.init_node('gossip', anonymous=True)

    param_rate = rospy.get_param('~rate', 1)
    param_log_message = rospy.get_param('~log_message', 'I am your Log')
    param_log_type = rospy.get_param('~log_type', 'info')

    rate = rospy.Rate(param_rate)

    while not rospy.is_shutdown():

        if param_log_type == 'info':
            rospy.loginfo(param_log_message)

        elif param_log_type == 'debug':
            rospy.logdebug(param_log_message)

        elif param_log_type == 'warn':
            rospy.logwarn(param_log_message)

        elif param_log_type == 'err':
            rospy.logerr(param_log_message)

        elif param_log_type == 'fatal':
            rospy.logfatal('I am a log fatal')

        rate.sleep()

if __name__ == '__main__':

    try:
        gossip()

    except rospy.ROSInterruptException:
        pass
