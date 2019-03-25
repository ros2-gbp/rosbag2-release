Source: @(Package)
Section: misc
Priority: extra
Maintainer: @(Maintainer)
Build-Depends: debhelper (>= @(debhelper_version).0.0), python-rospkg, ros-melodic-actionlib-msgs, ros-melodic-catkin, ros-melodic-common-msgs, ros-melodic-geometry-msgs, ros-melodic-nav-msgs, ros-melodic-rosbash, ros-melodic-roscpp, ros-melodic-roscpp-tutorials, ros-melodic-roslaunch, ros-melodic-rosmsg, ros-melodic-rospy-tutorials, ros-melodic-sensor-msgs, ros-melodic-std-msgs, ros-melodic-std-srvs, ros-melodic-stereo-msgs, ros-melodic-tf2-msgs, ros-melodic-trajectory-msgs, ros-melodic-visualization-msgs, @(', '.join(BuildDepends))
Homepage: @(Homepage)
Standards-Version: 3.9.2

Package: @(Package)
Architecture: any
Depends: ${shlibs:Depends}, ${misc:Depends}, python-rospkg, ros-melodic-actionlib-msgs, ros-melodic-catkin, ros-melodic-common-msgs, ros-melodic-geometry-msgs, ros-melodic-nav-msgs, ros-melodic-rosbash, ros-melodic-roscpp, ros-melodic-roscpp-tutorials, ros-melodic-roslaunch, ros-melodic-rosmsg, ros-melodic-rospy-tutorials, ros-melodic-sensor-msgs, ros-melodic-std-msgs, ros-melodic-std-srvs, ros-melodic-stereo-msgs, ros-melodic-tf2-msgs, ros-melodic-trajectory-msgs, ros-melodic-visualization-msgs, @(', '.join(Depends))
@[if Conflicts]Conflicts: @(', '.join(Conflicts))@\n@[end if]@
@[if Replaces]Replaces: @(', '.join(Replaces))@\n@[end if]@
Description: @(Description)
