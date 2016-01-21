/******************************************************************************/
/*                                                                            */
/*            H A N D L E   M Q   B A C K O U T   M E S S A G E S             */
/*                              B C K H N D . C                               */
/*                                                                            */
/* -------------------------------------------------------------------------- */
/*                                                                            */
/*  functions:                              */
/*    - backoutHandler                      */
/*    - moveMessages              */
/*                              */
/******************************************************************************/

/******************************************************************************/
/*   I N C L U D E S                                                          */
/******************************************************************************/

// ---------------------------------------------------------
// system
// ---------------------------------------------------------
#include <errno.h>
#include <stdlib.h>

// ---------------------------------------------------------
// MQ
// ---------------------------------------------------------
#include <cmqc.h>

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
#include <ctl.h>
#include <msgcat/lgstd.h>
#include <mqbase.h>

// ---------------------------------------------------------
// local
// ---------------------------------------------------------
#include <bckhnd.h>
#include <cmdln.h>

/******************************************************************************/
/*   G L O B A L S                                                            */
/******************************************************************************/

/******************************************************************************/
/*   D E F I N E S                                                            */
/******************************************************************************/
#define INITIAL_MSG_SIZE 4096

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/
int moveMessages();

/******************************************************************************/
/*                                                                            */
/*   F U N C T I O N S                                                        */
/*                                                                            */
/******************************************************************************/

/******************************************************************************/
/*  back out handler                    */
/******************************************************************************/
int backoutHandler()
{
  logFuncCall() ;               

  int sysRc = 0 ;

  MQHCONN hCon  ;                   // connection handle   
  char qmgrName[MQ_Q_MGR_NAME_LENGTH+1];
                                    //
  MQHOBJ  hBoq ;                    // queue handle   
  MQOD    dBoq = {MQOD_DEFAULT};    // queue descriptor
  char boqName[MQ_Q_NAME_LENGTH+1]; //
                              //
  MQHOBJ  hSrcq ;                    // queue handle   
  MQOD    dSrcq = {MQOD_DEFAULT};    // queue descriptor
  char srcqName[MQ_Q_NAME_LENGTH+1]; //
                            //
  MQHOBJ  hFwdq ;                    // queue handle   
  MQOD    dFwdq = {MQOD_DEFAULT};    // queue descriptor
  char fwdqName[MQ_Q_NAME_LENGTH+1]; //

  // -------------------------------------------------------
  // connect to queue manager 
  // -------------------------------------------------------
  memset( qmgrName, ' ', MQ_Q_MGR_NAME_LENGTH );  
  qmgrName[MQ_Q_MGR_NAME_LENGTH] = '\0'  ;  // copy queue manager name
  memcpy( qmgrName                       ,  // from command line attributes
          getStrAttr( "qmgr" )           ,  // to vara
          strlen( getStrAttr( "qmgr" ) ));  //
                                            //
  sysRc =  mqConn( (char*) qmgrName,        // queue manager          
                           &hCon  );        // connection handle            
                                            //
  switch( sysRc )                           //
  {                                         //
    case MQRC_NONE :     break ;            // OK
    case MQRC_Q_MGR_NAME_ERROR :            // queue manager does not exists
    {                                       //
      logger(LMQM_UNKNOWN_QMGR,qmgrName);   //
      goto _door;                           //
    }                                       //
    default : goto _door;                   // error will be logged in mqConn
  }                                         //
                                            //
  // -------------------------------------------------------
  // open back out queue
  // -------------------------------------------------------
  memset( boqName, ' ', MQ_Q_NAME_LENGTH );   //
  boqName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dBoq.ObjectName   ,                 //
          boqName           ,                 //
          MQ_Q_NAME_LENGTH );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dBoq                 , // queue descriptor
                      MQOO_INPUT_EXCLUSIVE  | // open for exclusive get
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
                      &hBoq );                // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open (original) source queue
  // -------------------------------------------------------
  memset( srcqName, ' ', MQ_Q_NAME_LENGTH );  //
  boqName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dSrcq.ObjectName  ,                 //
          srcqName          ,                 //
          MQ_Q_NAME_LENGTH );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dSrcq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
                      &hSrcq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open (original) source queue
  // -------------------------------------------------------
  memset( fwdqName, ' ', MQ_Q_NAME_LENGTH );  //
  boqName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dFwdq.ObjectName  ,                 //
          fwdqName          ,                 //
          MQ_Q_NAME_LENGTH );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dFwdq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // open options
                      &hFwdq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // move messages under sync point
  // -------------------------------------------------------
  sysRc = moveMessages( hCon );

  _door:

  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*   move messages             */
/******************************************************************************/
int moveMessages( MQHCONN _hCon ,     // connection handle   
                  MQHOBJ  _hGetQ,     // get queue handle
                  MQHOBJ  _hPutOrig   // get queue handle
		 )              
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;

  MQMD  md  = {MQMD_DEFAULT} ;    // message descriptor (set to default)
  MQGMO gmo = {MQGMO_DEFAULT};     // get message option set to default
//MQPMO pmo = {MQPMO_DEFAULT};     // put message option set to default
                                   //
                        //
  static PMQVOID *buffer = NULL;  // message buffer, allocation necessary only 
                                  // on first call of move messages since static
  static MQLONG msgLng     = INITIAL_MSG_SIZE ;
  static MQLONG bufferSize = INITIAL_MSG_SIZE; 

  // -----------------------------------------------------
  // initialization of static vara with first call of this function
  // -----------------------------------------------------
  if( !buffer )                             //
  {                                         //
    buffer = (PMQVOID) malloc( msgLng );    //
    if( !buffer )                           //
    {                                       //
      logger( LSTD_MEM_ALLOC_ERROR );       //
      sysRc = errno ;                       //
      goto _door;                           //
    }                                       //
  }                                         //
                                            //
  // -----------------------------------------------------
  // start transaction
  // -----------------------------------------------------
  reason = mqBegin( _hCon );                // begin transaction
  switch( reason )                          //
  {                                         //
    case MQRC_NONE :                        //
    case MQRC_NO_EXTERNAL_PARTICIPANTS :    // transactions without external 
    {                                       //  resource manager
      sysRc = MQRC_NONE;                    //
      break;                                //
    }                                       //
    default : goto _door;                   //
  }                                         //
                                            // 
  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  memcpy( &md.MsgId         ,               // flash message id, 
          MQMI_NONE         ,               // 
          sizeof(md.MsgId) );               // 
  md.Version = MQMD_VERSION_2;              //
                                            //
  gmo.MatchOptions = MQMO_MATCH_MSG_ID;     // 
  gmo.Options      = MQGMO_CONVERT    +     //
                     MQGMO_SYNCPOINT  ,     //
                     MQGMO_WAIT       ;     //
  gmo.Version      = MQGMO_VERSION_3  ;     //
                                            //
  reason = mqGet( _hCon    ,                // connection handle
                  _hGetQ   ,                // pointer to queue handle
                  buffer   ,                // message buffer
                  &msgLng  ,                // buffer length
                  &md      ,                // message descriptor
                  gmo      ,                // get message option
                  60000   );                // wait interval in milliseconds
                                            // (makes 1Minute)
  switch( reason )                          //
  {                                         //
    // ---------------------------------------------------
    // read was OK
    // ---------------------------------------------------
    case MQRC_NONE :                        //
    {                                       //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // no message found -> OK
    // ---------------------------------------------------
    case MQRC_NO_MSG_AVAILABLE :            // no message available, necessary 
    {                                       //  for catching signals
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // an error occurred preventing later commit
    // ---------------------------------------------------
    case MQRC_BACKED_OUT:                   //
    {                                       //
      sysRc = reason;                       //
      reason = mqRollback( _hCon );         // roll back 
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
      }                                     //
      goto _door;                           //
    }                                       //
                                            //
    // ---------------------------------------------------
    // message buffer to small for the (physical) message 
    //   - rollback
    //   - resize buffer
    //   - re-read message
    // ---------------------------------------------------
    case MQRC_TRUNCATED_MSG_FAILED :        // 
    {                                       //
      logMQCall( WAR, "MQGET", reason );    //
                                            //
      sysRc = reason;                       //
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // resize buffer
      // -----------------------------------// increase the new size to
      bufferSize=((int)(msgLng/1024)+1)*1024;   // next full kB, 
      buffer=resizeMqMessageBuffer(buffer     , // f.e. 4500Byte to 5k
                                  &bufferSize );// reallocating the buffer
      if( !buffer )                         //
      {                                     //
        sysRc = errno ;                     //
        if( sysRc == 0 ) sysRc = 1 ;        //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // read the message with new buffer size
      // -------------------------------------------------
      reason = mqGet( _hCon      ,          // connection handle
                      _hGetQ     ,          // pointer to queue handle
                      buffer     ,          // message buffer
                      &msgLng    ,          // buffer length
                      &md        ,          // message descriptor
                      gmo        ,          // get message option
                      5000      );          // wait interval in milliseconds
                                            //
      switch( reason )                      //
      {                                     //
        case MQRC_NONE: break;              //
        default:                            //
        {                                   //
	  sysRc = reason;                   //
          goto _door;                       //
        }                                   //
      }                                     //
    }                                       //
  }                                         //

  _door:

  logFuncExit( );
  return sysRc ;
}
