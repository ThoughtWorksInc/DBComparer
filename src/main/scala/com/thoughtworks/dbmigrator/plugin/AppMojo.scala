package com.thoughtworks.dbmigrator.plugin

import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations.Mojo

@Mojo(name = "hello")
class AppMojo extends AbstractMojo{
  override def execute() = {
    println( "Hello World!" )
  }
}
