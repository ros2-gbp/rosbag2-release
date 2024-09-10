%bcond_without tests
%bcond_without weak_deps

%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')
%global __provides_exclude_from ^/opt/ros/jazzy/.*$
%global __requires_exclude_from ^/opt/ros/jazzy/.*$

Name:           ros-jazzy-rosbag2-cpp
Version:        0.26.5
Release:        1%{?dist}%{?release_suffix}
Summary:        ROS rosbag2_cpp package

License:        Apache License 2.0
Source0:        %{name}-%{version}.tar.gz

Requires:       ros-jazzy-ament-index-cpp
Requires:       ros-jazzy-pluginlib
Requires:       ros-jazzy-rclcpp
Requires:       ros-jazzy-rcpputils
Requires:       ros-jazzy-rcutils
Requires:       ros-jazzy-rmw
Requires:       ros-jazzy-rmw-implementation
Requires:       ros-jazzy-rosbag2-storage
Requires:       ros-jazzy-rosidl-runtime-c
Requires:       ros-jazzy-rosidl-runtime-cpp
Requires:       ros-jazzy-rosidl-typesupport-cpp
Requires:       ros-jazzy-rosidl-typesupport-introspection-cpp
Requires:       ros-jazzy-shared-queues-vendor
Requires:       ros-jazzy-ros-workspace
BuildRequires:  ros-jazzy-ament-cmake
BuildRequires:  ros-jazzy-ament-index-cpp
BuildRequires:  ros-jazzy-pluginlib
BuildRequires:  ros-jazzy-rclcpp
BuildRequires:  ros-jazzy-rcpputils
BuildRequires:  ros-jazzy-rcutils
BuildRequires:  ros-jazzy-rmw
BuildRequires:  ros-jazzy-rmw-implementation
BuildRequires:  ros-jazzy-rosbag2-storage
BuildRequires:  ros-jazzy-rosidl-runtime-c
BuildRequires:  ros-jazzy-rosidl-runtime-cpp
BuildRequires:  ros-jazzy-rosidl-typesupport-cpp
BuildRequires:  ros-jazzy-rosidl-typesupport-introspection-cpp
BuildRequires:  ros-jazzy-shared-queues-vendor
BuildRequires:  ros-jazzy-ros-workspace
Provides:       %{name}-devel = %{version}-%{release}
Provides:       %{name}-doc = %{version}-%{release}
Provides:       %{name}-runtime = %{version}-%{release}

%if 0%{?with_tests}
BuildRequires:  ros-jazzy-ament-cmake-gmock
BuildRequires:  ros-jazzy-ament-lint-auto
BuildRequires:  ros-jazzy-ament-lint-common
BuildRequires:  ros-jazzy-rosbag2-storage-default-plugins
BuildRequires:  ros-jazzy-rosbag2-test-common
BuildRequires:  ros-jazzy-rosbag2-test-msgdefs
BuildRequires:  ros-jazzy-std-msgs
BuildRequires:  ros-jazzy-test-msgs
%endif

%description
C++ ROSBag2 client library

%prep
%autosetup -p1

%build
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/jazzy/setup.sh" ]; then . "/opt/ros/jazzy/setup.sh"; fi
mkdir -p .obj-%{_target_platform} && cd .obj-%{_target_platform}
%cmake3 \
    -UINCLUDE_INSTALL_DIR \
    -ULIB_INSTALL_DIR \
    -USYSCONF_INSTALL_DIR \
    -USHARE_INSTALL_PREFIX \
    -ULIB_SUFFIX \
    -DCMAKE_INSTALL_PREFIX="/opt/ros/jazzy" \
    -DAMENT_PREFIX_PATH="/opt/ros/jazzy" \
    -DCMAKE_PREFIX_PATH="/opt/ros/jazzy" \
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
if [ -f "/opt/ros/jazzy/setup.sh" ]; then . "/opt/ros/jazzy/setup.sh"; fi
%make_install -C .obj-%{_target_platform}

%if 0%{?with_tests}
%check
# Look for a Makefile target with a name indicating that it runs tests
TEST_TARGET=$(%__make -qp -C .obj-%{_target_platform} | sed "s/^\(test\|check\):.*/\\1/;t f;d;:f;q0")
if [ -n "$TEST_TARGET" ]; then
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/jazzy/setup.sh" ]; then . "/opt/ros/jazzy/setup.sh"; fi
CTEST_OUTPUT_ON_FAILURE=1 \
    %make_build -C .obj-%{_target_platform} $TEST_TARGET || echo "RPM TESTS FAILED"
else echo "RPM TESTS SKIPPED"; fi
%endif

%files
/opt/ros/jazzy

%changelog
* Tue Sep 10 2024 Michael Orlov <michael.orlov@apex.ai> - 0.26.5-1
- Autogenerated by Bloom

* Fri Jun 28 2024 Michael Orlov <michael.orlov@apex.ai> - 0.26.4-1
- Autogenerated by Bloom

* Thu May 16 2024 Michael Orlov <michael.orlov@apex.ai> - 0.26.3-1
- Autogenerated by Bloom

* Wed Apr 24 2024 Michael Orlov <michael.orlov@apex.ai> - 0.26.2-1
- Autogenerated by Bloom

* Fri Apr 19 2024 Michael Orlov <michael.orlov@apex.ai> - 0.26.1-2
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

