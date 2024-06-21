%bcond_without tests
%bcond_without weak_deps

%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')
%global __provides_exclude_from ^/opt/ros/rolling/.*$
%global __requires_exclude_from ^/opt/ros/rolling/.*$

Name:           ros-rolling-rosbag2-transport
Version:        0.28.0
Release:        1%{?dist}%{?release_suffix}
Summary:        ROS rosbag2_transport package

License:        Apache License 2.0
Source0:        %{name}-%{version}.tar.gz

Requires:       ros-rolling-keyboard-handler
Requires:       ros-rolling-rclcpp
Requires:       ros-rolling-rclcpp-components
Requires:       ros-rolling-rmw
Requires:       ros-rolling-rosbag2-compression
Requires:       ros-rolling-rosbag2-cpp
Requires:       ros-rolling-rosbag2-interfaces
Requires:       ros-rolling-rosbag2-storage
Requires:       ros-rolling-shared-queues-vendor
Requires:       ros-rolling-yaml-cpp-vendor
Requires:       ros-rolling-ros-workspace
BuildRequires:  ros-rolling-ament-cmake-ros
BuildRequires:  ros-rolling-keyboard-handler
BuildRequires:  ros-rolling-rclcpp
BuildRequires:  ros-rolling-rclcpp-components
BuildRequires:  ros-rolling-rmw
BuildRequires:  ros-rolling-rosbag2-compression
BuildRequires:  ros-rolling-rosbag2-cpp
BuildRequires:  ros-rolling-rosbag2-interfaces
BuildRequires:  ros-rolling-rosbag2-storage
BuildRequires:  ros-rolling-shared-queues-vendor
BuildRequires:  ros-rolling-yaml-cpp-vendor
BuildRequires:  ros-rolling-ros-workspace
Provides:       %{name}-devel = %{version}-%{release}
Provides:       %{name}-doc = %{version}-%{release}
Provides:       %{name}-runtime = %{version}-%{release}

%if 0%{?with_tests}
BuildRequires:  ros-rolling-ament-cmake-gmock
BuildRequires:  ros-rolling-ament-index-cpp
BuildRequires:  ros-rolling-ament-lint-auto
BuildRequires:  ros-rolling-ament-lint-common
BuildRequires:  ros-rolling-composition-interfaces
BuildRequires:  ros-rolling-rmw-implementation-cmake
BuildRequires:  ros-rolling-rosbag2-compression-zstd
BuildRequires:  ros-rolling-rosbag2-storage-default-plugins
BuildRequires:  ros-rolling-rosbag2-test-common
BuildRequires:  ros-rolling-test-msgs
%endif

%description
Layer encapsulating ROS middleware to allow rosbag2 to be used with or without
middleware

%prep
%autosetup -p1

%build
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/rolling/setup.sh" ]; then . "/opt/ros/rolling/setup.sh"; fi
mkdir -p .obj-%{_target_platform} && cd .obj-%{_target_platform}
%cmake3 \
    -UINCLUDE_INSTALL_DIR \
    -ULIB_INSTALL_DIR \
    -USYSCONF_INSTALL_DIR \
    -USHARE_INSTALL_PREFIX \
    -ULIB_SUFFIX \
    -DCMAKE_INSTALL_PREFIX="/opt/ros/rolling" \
    -DAMENT_PREFIX_PATH="/opt/ros/rolling" \
    -DCMAKE_PREFIX_PATH="/opt/ros/rolling" \
    -DSETUPTOOLS_DEB_LAYOUT=OFF \
%if !0%{?with_tests}
    -DBUILD_TESTING=OFF \
%endif
    ..

%make_build

%install
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/rolling/setup.sh" ]; then . "/opt/ros/rolling/setup.sh"; fi
%make_install -C .obj-%{_target_platform}

%if 0%{?with_tests}
%check
# Look for a Makefile target with a name indicating that it runs tests
TEST_TARGET=$(%__make -qp -C .obj-%{_target_platform} | sed "s/^\(test\|check\):.*/\\1/;t f;d;:f;q0")
if [ -n "$TEST_TARGET" ]; then
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/rolling/setup.sh" ]; then . "/opt/ros/rolling/setup.sh"; fi
CTEST_OUTPUT_ON_FAILURE=1 \
    %make_build -C .obj-%{_target_platform} $TEST_TARGET || echo "RPM TESTS FAILED"
else echo "RPM TESTS SKIPPED"; fi
%endif

%files
/opt/ros/rolling

%changelog
* Fri Jun 21 2024 Michael Orlov <michael.orlov@apex.ai> - 0.28.0-1
- Autogenerated by Bloom

* Tue Apr 30 2024 Michael Orlov <michael.orlov@apex.ai> - 0.27.0-1
- Autogenerated by Bloom

* Wed Apr 17 2024 Michael Orlov <michael.orlov@apex.ai> - 0.26.1-1
- Autogenerated by Bloom

* Tue Apr 16 2024 Michael Orlov <michael.orlov@apex.ai> - 0.26.0-1
- Autogenerated by Bloom

* Thu Mar 28 2024 Michael Orlov <michael.orlov@apex.ai> - 0.25.0-1
- Autogenerated by Bloom

* Mon Mar 11 2024 Michael Orlov <michael.orlov@apex.ai> - 0.24.0-3
- Autogenerated by Bloom

* Wed Mar 06 2024 Michael Orlov <michael.orlov@apex.ai> - 0.24.0-2
- Autogenerated by Bloom

