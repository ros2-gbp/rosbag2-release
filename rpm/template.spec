%bcond_without tests
%bcond_without weak_deps

%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')
%global __provides_exclude_from ^/opt/ros/rolling/.*$
%global __requires_exclude_from ^/opt/ros/rolling/.*$

Name:           ros-rolling-ros2bag
Version:        0.32.0
Release:        1%{?dist}%{?release_suffix}
Summary:        ROS ros2bag package

License:        Apache License 2.0
Source0:        %{name}-%{version}.tar.gz

Requires:       python%{python3_pkgversion}-yaml
Requires:       ros-rolling-ament-index-python
Requires:       ros-rolling-rclpy
Requires:       ros-rolling-ros2cli
Requires:       ros-rolling-rosbag2-py
Requires:       ros-rolling-ros-workspace
BuildRequires:  python%{python3_pkgversion}-devel
BuildRequires:  ros-rolling-ros-workspace
Provides:       %{name}-devel = %{version}-%{release}
Provides:       %{name}-doc = %{version}-%{release}
Provides:       %{name}-runtime = %{version}-%{release}

%if 0%{?with_tests}
BuildRequires:  python%{python3_pkgversion}-pytest
BuildRequires:  ros-rolling-ament-copyright
BuildRequires:  ros-rolling-ament-flake8
BuildRequires:  ros-rolling-ament-pep257
BuildRequires:  ros-rolling-ament-xmllint
BuildRequires:  ros-rolling-launch-testing
BuildRequires:  ros-rolling-launch-testing-ros
BuildRequires:  ros-rolling-rosbag2-storage-default-plugins
BuildRequires:  ros-rolling-rosbag2-test-common
%endif

%description
Entry point for rosbag in ROS 2

%prep
%autosetup -p1

%build
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/rolling/setup.sh" ]; then . "/opt/ros/rolling/setup.sh"; fi
%py3_build

%install
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/rolling/setup.sh" ]; then . "/opt/ros/rolling/setup.sh"; fi
%py3_install -- --prefix "/opt/ros/rolling"

%if 0%{?with_tests}
%check
# Look for a directory with a name indicating that it contains tests
TEST_TARGET=$(ls -d * | grep -m1 "\(test\|tests\)" ||:)
if [ -n "$TEST_TARGET" ] && %__python3 -m pytest --version; then
# In case we're installing to a non-standard location, look for a setup.sh
# in the install tree and source it.  It will set things like
# CMAKE_PREFIX_PATH, PKG_CONFIG_PATH, and PYTHONPATH.
if [ -f "/opt/ros/rolling/setup.sh" ]; then . "/opt/ros/rolling/setup.sh"; fi
%__python3 -m pytest $TEST_TARGET || echo "RPM TESTS FAILED"
else echo "RPM TESTS SKIPPED"; fi
%endif

%files
/opt/ros/rolling

%changelog
* Fri Apr 18 2025 Michael Orlov <michael.orlov@apex.ai> - 0.32.0-1
- Autogenerated by Bloom

* Tue Feb 04 2025 Michael Orlov <michael.orlov@apex.ai> - 0.31.0-1
- Autogenerated by Bloom

* Wed Sep 04 2024 Michael Orlov <michael.orlov@apex.ai> - 0.29.0-1
- Autogenerated by Bloom

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

